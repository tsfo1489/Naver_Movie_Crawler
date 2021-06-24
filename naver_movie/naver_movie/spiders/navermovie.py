from urllib import parse
import scrapy
import regex as re
from scrapy.http import request
from naver_movie.items import NaverMovieItem
import math


class NavermovieSpider(scrapy.Spider):
    name = 'navermovie'
    allowed_domains = ['movie.naver.com']
    code_re = re.compile('code=[0-9]+')
    movie_dir = 'https://movie.naver.com/movie/sdb/browsing/bmovie.nhn?'
    comment_page = 'https://movie.naver.com/movie/bi/mi/pointWriteFormList.nhn?'

    def start_requests(self):
        years = [2020]
        for y in years :
            yield scrapy.Request(url=(self.movie_dir + 'open={}&page=1'.format(y)), callback=self.parse_year)

    def parse_year(self, response):
        next_page = response.xpath('//td[contains(@class, "next")]').get()
        print(response.url)
        if not next_page is None :
            next_page = next_page[next_page.find('page=')+5: next_page.rfind('"')]
            if int(next_page) <= 10 :
                temp_url = response.url[:response.url.find('page=') + 5]
                yield scrapy.Request(url=(temp_url + next_page), callback=self.parse_year)
        movie_list = response.xpath('//*[@id="old_content"]/ul/li').getall()
        for m in movie_list :
            movie_code = int(self.code_re.search(m).group()[5:])
            yield scrapy.Request(url=(self.comment_page + 'code={}&page=1'.format(movie_code)), callback=self.parse_page)
 
    def parse_page(self, response) :
        page = int(response.url[response.url.find('page=') + 5:])
        comment_n = response.xpath('/html/body/div/div/div[3]/strong/em').get()
        if comment_n is None :
            print('Error at', response.url)
        comment_n = int(comment_n[4:-5].replace(',',''))

        max_n = min(comment_n - (page - 1) * 10, 10)
        for i in range(max_n) :
            doc = NaverMovieItem()
            doc['comment'] = response.xpath('//*[@id="_filtered_ment_{}"]/text()'.format(i)).get().strip()
            doc['movie_code'] = response.url[response.url.find('code=') + 5: response.url.find('&page=')]
            yield doc