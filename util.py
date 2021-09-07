# coding:utf8
import re
import os
import time
import queue



try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
import functools
import threading
import requests
import multiprocessing


import json
import redis
import pymongo
import tldextract

import log
import setting

logger = log.get_logger(__file__)

curpath = os.path.dirname(os.path.abspath(__file__))
pid_dir = os.path.join(curpath, "pid")
if not os.path.isdir(pid_dir):
    os.makedirs(pid_dir)


# 2020-06-18 添加
def get_task(task_redis_conn, task_key, limit):
    '''
    批量弹出任务
    '''
    pipe = task_redis_conn.pipeline()
    for i in range(limit):
        pipe.rpop(task_key)
    task_loop = pipe.execute()
    task_loop = [task for task in task_loop if task]
    return task_loop


def to_redis(task_redis_conn, task_info, post):
    '''
    数据推入redis
    '''
    try:
        # result_addr = task_info.get("result_addr", None)
        # if not result_addr:
        #     return 
        result_key = task_info.get("result_key")
        # redis_task_conn = redis.StrictRedis.from_url(result_addr)
        task_redis_conn.lpush(result_key, json.dumps(post))
    except Exception as e:
        logger.error("redis error")


def to_redis_hash(task_redis_conn, task_id, post, url):
    hash_addr = setting.UPDATE_TASK_DETAIL_RET_FORMAT.format(task_id)
    try:
        j_post= json.dumps(post, ensure_ascii=False)
        task_redis_conn.hset(hash_addr, url, j_post)
        task_redis_conn.lpush(setting.DPZ_BACKUP_LIST, j_post)
        task_redis_conn.expire(hash_addr, 86400 * 2)
    except:
        logger.error("hset error {0}".format(url))
        return -1
    return 1


def get_key_name(name):
    d = {
        "bbs.pcauto": "pcauto_bbs",
        "bbs1.people": "people_bbs1",
        "bbs.ppmoney": "ppmoney_bbs",
        "baijiahao": "baidubaijia",
        "10jqka": "10jqka_stock",
        "xcar": "xcar_bbs",
        "www.bilibili": "bilibili_www",
        "wenda.so": "so_wenda",
        "stock.eastmoney": "eastmoney_finance",
        "finance.eastmoney": "eastmoney_finance",
        "zhidao.baidu": "baidu_zhidao",
        "buluo.qq": "qq_buluo",
        "tz.fafengtuqiang": "fafengtuqiang_tz",
        "stock.jrj": "jrj_stock",
        "bbs.jrj": "jrj_bbs",
        "bbs.qianlong": "qianlong_bbs",
        "tousu.sina.com.cn": "sina_tousu",
        "cqwz.cqnews": "cqnews_cqwz",
        "wenwen.sogou": "sogou_wenwen",
        "qsy.fengniao": "fengniao_news",
        "qicai.fengniao": "fengniao_news",
        "bbs.fengniao": "fengniao_bbs",
        "www.gfan": "gfan_news",
        "bbs.gfan": "gfan_bbs",
        "iesdouyin": "douyin",
        "wenda.tianya.cn": "tianya_wenda",
        "ixigua.com": "365yg",
        "news.bitauto.com": "bitauto_news",
        "vc.yiche.com": "yiche_vc",
        "info.xcar.com.cn": "xcar_news",
        "aikahao.xcar.com.cn": "xcar_news",
        "drive.xcar.com.cn": "xcar_news",
        "www.pcauto.com.cn": "pcauto_www",
        "hj.pcauto.com.cn": "pcauto_hj",
        "mguba.eastmoney.com": "eastmoney_mguba",
        "zhuanlan.zhihu.com": "zhihu_zhuanlan",
        "bbs.lh168.net":"lh168_bbs",
        "www.lh168.net":"lh168_www",
    }
    key_name = d.get(name, None)
    if not key_name:
        return name
    else:
        return key_name

        
def get_web_queue(url, web_list):
    if "http" not in url:
        netloc = url
    else:
        # 获取url的域名
        url_res = urlparse(url)
        netloc = url_res.netloc
    # 判断网站是否在采集队列
    for web_name in web_list:
        if web_name in netloc:
            key_name = get_key_name(web_name)
            return key_name
    return None


def update_status(task_redis_conn, task_id, success=None, fail=None):
    is_new = True
    if task_id.startswith("backend"):
        is_new = False
    if not success and not fail:
        return
    if success:
        success_count = task_redis_conn.hincrby(setting.WEB_STATUS_KEY, str(task_id) + "_" + "success", amount=1)
        if is_new is True:
            fail_count = task_redis_conn.hget(setting.WEB_STATUS_KEY, str(task_id) + "_" + "failed")
        else:
            fail_count = task_redis_conn.hget(setting.WEB_STATUS_KEY, str(task_id) + "_" + "fail")
        fail_count = fail_count if fail_count else 0
    if fail:
        if is_new is True:
            fail_count = task_redis_conn.hincrby(setting.WEB_STATUS_KEY, str(task_id) + "_" + "failed", amount=1)
        else:
            fail_count = task_redis_conn.hincrby(setting.WEB_STATUS_KEY, str(task_id) + "_" + "fail", amount=1)
        success_count = task_redis_conn.hget(setting.WEB_STATUS_KEY, str(task_id) + "_" + "success")
        success_count = success_count if success_count else 0
    total_count = task_redis_conn.hget(setting.WEB_STATUS_KEY, str(task_id) + "_total_count")
    if total_count is None or total_count == "None":
        total_count = task_redis_conn.hget(setting.WEB_STATUS_KEY, str(task_id))
    logger.info(task_id)
    logger.info(total_count)

    amount = int(success_count) + int(fail_count)
    logger.info(amount)
    if int(total_count) == amount:
        task_redis_conn.hset(setting.WEB_STATUS_KEY, str(task_id) + "_" + "status", 2)
        task_redis_conn.hset(setting.WEB_STATUS_KEY, str(task_id) + "_" + "finish_time", int(time.time()))
        logger.info("任务 {0}完成 !!! ".format(task_id))


def DingTalk(msg):
    data = {
        "msgtype": "text",
        "text": {"content": msg
                 },
        "at": {
            "atMobiles": [13621232051],
            "isAtAll": False
        },
    }
    headers = {
        'Content-Type': 'application/json;charset=utf-8'
    }
    url = 'https://oapi.dingtalk.com/robot/send?access_token=a2a94dd08b11ce9afc678cf85941b26831228c33d52b0b7b2f95157f1557b08c'
    try:
        requests.post(url, json.dumps(data), headers=headers)
    except:
        pass


def log_error(e, func):
    logger.exception(e)
    return 1


def keepalive(handle_func=log_error, interval=1):
    '''装饰器
    功能：
       捕获被装饰函数的异常并重新调用函数
       函数正常结束则结束
    装饰器参数：
       @handle_func:function
          异常处理函数 默认接收参数 e(异常对象), func(被装饰函数)
       @interval:number
          函数重启间隔
    '''

    def wrapper(func):
        @functools.wraps(func)
        def keep(*args, **kwargs):
            while 1:
                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    if handle_func:
                        handle_func(e, func)
                    time.sleep(interval)
                    continue
                break
            return result

        return keep

    return wrapper


def launch_process(target, args, type=setting.PROCESS_MODE):
    if type == "thread":
        p = threading.Thread(target=target, args=args)
    elif type == "process":
        p = multiprocessing.Process(target=target, args=args)
    p.start()
    return p


def get_queue(type=setting.PROCESS_MODE):
    if type == "thread":
        return queue.Queue()
    elif type == "process":
        return multiprocessing.Queue()


def get_event(type=setting.PROCESS_MODE):
    if type == "thread":
        return threading.Event()
    elif type == "process":
        return multiprocessing.Event()


def set_pid(filename):
    filename = filename.split(os.sep)[-1].split(".")[0]
    filename = filename + ".pid"
    filename = os.path.join(pid_dir, filename)
    if os.path.exists(filename):
        return ""
    else:
        with open(filename, "w") as f:
            f.write("%s" % os.getpid())
        return filename


def check_stop(filename, stop_event):
    while 1:
        if os.path.exists(filename):
            time.sleep(2)
            continue
        else:
            stop_event.set()
            break
    return


def stop_process(filename):
    for pid_file in os.listdir(pid_dir):
        if filename in pid_file or filename == "all":
            os.remove(os.path.join(pid_dir, pid_file))
    return 1



# 域名-队列字典
domain_queue_dict = {
    'sina.com.cn': "comment_task:sina",
    'toutiao.com': "comment_task:toutiao",
    'tianya.cn': "comment_task:tianya",
    'ifeng.com': "comment_task:ifeng",
    '163.com': "comment_task:163",
    'qq.com': "comment_task:qq",
    # 'weibo.com': "comment_task:weibo",
    'sohu.com': "comment_task:sohu",
    'baidu.com': "comment_task:tieba",
    'xici.net': "comment_task:xici",
}

# 域名-子域名白名单列表, 如果为空，则所有的子域名都可以
domain_subdomain_dict = {
    'sina.com.cn': ['news', ],
    'toutiao.com': [],
    'tianya.cn': ['bbs', ],
    'ifeng.com': ['news', ],
    '163.com': ['news', ],
    'qq.com': ['news', 'new', ],
    'weibo.com': [],
    'sohu.com': [],
    'baidu.com': ['tieba'],
    'miaopai.com': [],
    'xici.net': [],
}



def get_queue_name(url):
    # 寻找队列

    subdomain, domain, suffix = tldextract.extract(url)
    domain = '.'.join([domain, suffix])
    # 获取队列名
    queue_name = domain_queue_dict.get(domain)
    if not queue_name:
        return {}

    # 查看是否在白名单里
    allowed_subdomains = domain_subdomain_dict.get(domain)

    # 如果allowed_subdomains 为空，则不进行白名单校验
    if allowed_subdomains:
        for allowed_subdomain in allowed_subdomains:
            # 如果子域名在白名单里，break跳出循环，
            # 这样写，而不直接写 if subdomain in allowed_subdomain，是为了解决三级域名的问题
            if subdomain in allowed_subdomain:
                break
        # 否则，不抓取评论
        else:
            return {}

    # 微博特殊处理
    if queue_name == 'comment_task:weibo':
        if not check_weibo_url(url):
            return {}
    # 是否发布新任务 目前针对贴吧
    dispatch_task = 1
    if queue_name in ['comment_task:tieba', 'comment_task:weixin']:
        dispatch_task = 0
    return {'queue_name': queue_name, "dispatch_task": dispatch_task}


weibo_accounts = {}


def init_weibo_accounts():
    global weibo_accounts
    conn = redis.StrictRedis.from_url("redis://192.168.16.223/8")
    uids = conn.lrange('weibo_uids', 0, -1)
    weibo_accounts = {}.fromkeys(uids)
    return

def check_weibo_url(url):
    if not weibo_accounts:
        init_weibo_accounts()
    uid = re.search('weibo.com/(\d+)/', url)
    uid = '' if not uid else uid.group(1)
    return uid in weibo_accounts


def get_d(seconds):
    """时间差"""
    d = ""
    if seconds <= 60:
        d = "d1"
    elif seconds <= 120:
        d = "d2"
    elif seconds <= 300:
        d = "d3"
    elif seconds <= 900:
        d = "d4"
    elif seconds <= 1800:
        d = "d5"
    elif seconds <= 3600:
        d = "d6"
    elif seconds <= 7200:
        d = "d7"
    elif seconds <= 14400:
        d = "d8"
    else:
        d = "d9"
    return d


def clean_mongo_news():
    """清理过期news信息"""
    p = urlparse(setting.DEFAULT_MONGODB_URI)
    mongo_client = pymongo.MongoClient(host=p.hostname, port=p.port)
    collection = mongo_client[setting.NEWS_DB][setting.NEWS_COLLECTION]

    total = 0
    _del = 0
    while 1:
        try:
            finder = collection.find({}, {"url": 1, "update_ts": 1})
            has_flag = 0
            for item in finder:
                has_flag = 1
                total += 1
                update_ts = item.get("update_ts")
                if not update_ts:
                    continue
                if update_ts < (time.time() - 86400 * 30):
                    collection.remove({"url": item.get("url")})
                    _del += 1
                print("%s: %s" % (total, _del))
            if not has_flag:
                break
        except Exception as e:
            print(e)
            time.sleep(10)

    return


def change_prior(pre_info, after_info, data):
    return

if __name__ == "__main__":
    # url = 'http://blog.sina.com.cn/dfasd/fasdfafasd'
    # url = 'http://mp.weixin.qq.com/s/4dkaOWtEw-weLBI73A0JzQ'
    # url = 'http://fasdfasdf.miaopai.com/fadfs'
    # # url = 'http://tj.auto.sina.com.cn/shcs/2017-06-02/detail-ifyfuzmy1218320.shtml'
    # print(get_queue_name(url))

    # print(weibo_accounts.keys()[:100])

    # print(check_weibo_url("http://weibo.com/176401185200/dfas"))
    clean_mongo_news()
