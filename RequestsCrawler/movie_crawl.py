import requests
import threading
import queue
import math
import time
from tqdm import tqdm
from bs4 import BeautifulSoup, Tag, NavigableString

class file_writer(threading.Thread) :
    def __init__(self, file, queue) :
        threading.Thread.__init__(self)
        self.file = file
        self.write_q = queue
        self.file.write('comment, movie_code\n')
    def run(self) :
        while True :
            comment, movie_code = self.write_q.get()
            if comment == '/*-+' :
                break
            comment = comment.replace(',', ' ')
            f.write('{}, {}\n'.format(comment, movie_code))

def movie_analyze(movie_code, page_n) :
    movie_url = 'https://movie.naver.com/movie/bi/mi/pointWriteFormList.nhn?code={}&page={}'.format(movie_code, page_n)
    total_cnt = 0
    ans = []
    with requests.get(movie_url) as res :
        if res.status_code == 200 :
            html = res.text
            soup = BeautifulSoup(html, 'html.parser')
            score_list = soup.select_one('div.input_netizen > div.score_result > ul')
            score_list = score_list.select('li')
            comment_cnt = 0
            comment_n = soup.select_one('body > div > div > div.score_total > strong > em').string                
            comment_n = int(comment_n.replace(',', ''))
            for comment_tag in score_list :
                comment = comment_tag.select_one('div.score_reple > p > span#_filtered_ment_{}'.format(comment_cnt)).text
                comment = comment.strip()
                ans.append([comment, movie_code])
                comment_cnt += 1
                total_cnt += 1
        return ans


class Crawler(threading.Thread) :
    def __init__(self, id, i_q, o_q) :
        threading.Thread.__init__(self)
        self.id = id
        self.input_q = i_q
        self.output_q = o_q
        self.done = 0
    def run(self) :
        while True :
            movie_code, page = self.input_q.get()
            if movie_code == -1 :
                break
            comment_list = movie_analyze(movie_code, page)
            self.done += len(comment_list)
            for comment in comment_list :
                self.output_q.put(comment)

class Spectator(threading.Thread) :
    def __init__(self, threads, n) :
        threading.Thread.__init__(self)
        self.threads = threads
        self.n = n
        self.pbar = tqdm(total=self.n)
    def run(self) :
        prev = 0
        while True :
            cnt = 0
            for t in self.threads :
                cnt += t.done
            self.pbar.update(cnt - prev)
            prev = cnt
            time.sleep(0.5)

if __name__ == '__main__' :
    f = open('output.csv','w', encoding='utf-8')
    write_q = queue.Queue()
    movie_q = queue.Queue()
    writer = file_writer(f, write_q)
    N_CRAWL = 32
    TARGET_COMMENT = 200000

    writer.daemon = True
    writer.start()
    crawlers = []
    for i in range(N_CRAWL) :
        crawlers.append(Crawler(i, movie_q, write_q))
        crawlers[-1].daemon = True

    movie_list = [] 
    movie_rank_url = 'https://movie.naver.com/movie/sdb/browsing/bmovie.nhn?open=2020&page='
    for i in range(70) :
        with requests.get(movie_rank_url + str(i+1)) as res :
            if res.status_code == 200 :
                html = res.text
                soup = BeautifulSoup(html, 'html.parser')
                rank_list = soup.select('#old_content > ul > li')
                for r in rank_list :
                    tmp = r.select_one('a')
                    if not tmp is None :
                        tmp = tmp.get('href')
                        movie_list.append(int(tmp[tmp.find('code=') + 5:]))

    
    total_cnt = 0
    movie_n = 0
    for movie in movie_list :
        if TARGET_COMMENT < total_cnt :
            break
        movie_url = 'https://movie.naver.com/movie/bi/mi/pointWriteFormList.nhn?code={}'.format(movie)
        with requests.get(movie_url) as res :
            if res.status_code == 200 :
                html = res.text
                soup = BeautifulSoup(html, 'html.parser')
                comment_n = soup.select_one('body > div > div > div.score_total > strong > em').string                
                comment_n = int(comment_n.replace(',', ''))
                total_cnt += comment_n
                movie_n += 1
                print('Movie({:>6}) has {:>5} comments / Total {} movies {} comments'.format(movie, comment_n, movie_n, total_cnt))
                for i in range(math.ceil(comment_n / 10)) :
                    movie_q.put([movie, i + 1])



    sp = Spectator(crawlers, total_cnt)
    sp.daemon = True
    sp.start()
    for t in crawlers :
        t.start()

    for i in range(N_CRAWL) :
        movie_q.put([-1, -1])
    for i in range(N_CRAWL) :
        crawlers[i].join()
    writer.write_q.put(['/*-+', -1])
    writer.join()
    f.close()
