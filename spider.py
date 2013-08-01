#coding=utf8 
import urllib
from pyquery import PyQuery as pq

product_url = 'http://www.pba.cn/product-306.html'
product_detail_url = 'http://www.pba.cn/product-goodsBodyContent-%s.html'
PATH = '/home/ajie/pba.jpg'

#直接用以下pq方法，仅返回html的head部分代码，貌似是该网站的代码不规范造成。其他网站可以
#pba_html = pq(url='http://www.pba.cn/product-307.html')

def pyQueryUrl(url):
    '''用pyquery解析url'''
    html = urllib.urlopen(url).read()
    jq_html = pq(unicode(html,'utf-8'))
    return jq_html

def storeImg(path, url):
    '''下载图片'''
    data = urllib.urlopen(url).read()
    f = open(path,'wb')
    f.write(data)
    f.close()

def main(url, path):
    product = {}
    #解析html
    jq = pyQueryUrl(url)
    product['title']  = jq('#spxqitem .content table table tr:eq(1)').children('td').eq(1).text()
    product['type']   = jq('#spxqitem .content table table tr:eq(2)').children('td').eq(3).text()
    product['volume'] = jq('#spxqitem .content table table tr:eq(3)').children('td').eq(1).text()
    product['intro']  = jq('#spxqitem .content table table:eq(1)').text()
    product['pic']    = jq('#spxqitem .content table tr:eq(0)').children('td').eq(1).find('img').attr('img-lazyload')
    
    result = ''
    for key in product.keys():
        print '%s:%s' % (key,product[key])
        if product[key] is None:
            result = 'failed'

    if result != 'failed':
        storeImg(path,product['pic'])
        print '\n\n--------------------------成功----------------------------\n'
    else:
        print '\n\n--------------------------失败----------------------------\n'

if __name__ == '__main__':
    file = open('/home/ajie/product_id.txt')
    for line in file:
        url = product_detail_url % int(line)
        print u'开始抓取id为%s的产品......' % int(line)
        main(url, PATH)
