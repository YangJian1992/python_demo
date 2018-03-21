import requests
#下载网页
class HtmlDownloader(object):
    def download(self, url):
        if url is None:
            return None
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0'
        headers = {'User-Agent':user_agent}
        r = requests.get(url, headers=headers)
        if r.status_code==200:#没有乱码
            r.encoding = 'utf-8'
            return  r.text
        return  None