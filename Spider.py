#coding=utf8 
import urllib, web
from DBHelper import DBHelper as db
from pyquery import PyQuery as pq

product_url = 'http://www.xxx.com/product-goodsBodyContent-%s.html'
MAKEUP_COVER_PATH = 'web/img/makeup/cover/'
MAKEUP_COVER_URL = '/img/makeup/cover/'


#直接用以下pq方法，仅返回html的head部分代码，貌似是该网站的代码不规范造成。其他网站可以
#pba_html = pq(url='http://www.xxx.com/product-307.html')

def pyQueryUrl(url):
    '''用pyquery解析url'''
    html = urllib.urlopen(url).read()
    jq_html = pq(unicode(html,'utf-8'))
    return jq_html

def saveImg(path, url, itemid):
    '''下载图片'''
    suffix = url.rpartition('.')[2].lower()
    assert(suffix in ['jpg','png','jpeg'])
    file_path = '%s%d.%s' % (path, itemid, suffix)

    data = urllib.urlopen(url).read()
    open(file_path, 'w').write(data)
    
    query = 'update Makeup set cover_url = %s where Makeupid = %s'
    values = [MAKEUP_COVER_URL+str(itemid)+'.'+suffix, itemid]
    db().update(query, tuple(values))

def main(url, path):
    product = {}
    #解析html
    jq = pyQueryUrl(url)
    product['name']  = jq('#spxqitem .content table table tr:eq(1)').children('td').eq(1).text()
    product['search_key']   = jq('#spxqitem .content table table tr:eq(2)').children('td').eq(3).text()
    product['volume'] = jq('#spxqitem .content table table tr:eq(3)').children('td').eq(1).text()
    product['intro']  = jq('#spxqitem .content table table').eq(1).text()
    product['cover_url']    = jq('#spxqitem .content table tr:eq(0)').children('td').eq(1).find('img').attr('img-lazyload')
    result = ''
    for key in product.keys():
        if product[key] is None:
            result = 'failed'
            break
        else:
            print '%s:%s' % (key,product[key])
    if result != 'failed':
        insertMysql(product)
    else:
        print '\n-----失败-----\n'

def insertMysql(data):
    query = 'insert into Makeup(name,intro,volume,search_key,Brandid) values(%s,%s,%s,%s,%s)'
    values = [ data['name'], data['intro'], data['volume'], 'pba '+data['search_key'], '3133']
    new_id = db().insert(query, tuple(values))
    saveImg(MAKEUP_COVER_PATH, data['cover_url'], new_id)
    print '\n-----成功-----\n'

if __name__ == '__main__':
    file = open('/home/ajie/PyQuerySpider/product_id.txt')
    for line in file:
        url = product_url % int(line)
        print u'开始抓取id为%s的产品......' % int(line)
        main(url, PATH)
