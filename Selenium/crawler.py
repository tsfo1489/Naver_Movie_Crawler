from os import write
from selenium import webdriver
from bs4 import BeautifulSoup
from math import ceil
from urllib3.packages.six import u
from tqdm import tqdm
from multiprocessing import Process, Queue

main_url = 'https://movie.naver.com/movie/sdb/browsing/bmovie.nhn?open=2020&page='
page_url = 'https://movie.naver.com/movie/bi/mi/pointWriteFormList.nhn?code='

def file_writer(output_q : Queue, comment_n : int) :
    f = open('output.csv', 'w', encoding='utf-8')
    f.write('comment, movie_code\n')
    pbar = tqdm(total=comment_n)
    while True :
        comment, movie_code = output_q.get()
        if movie_code == -1 :
            break
        comment = comment.replace(',', ' ')
        f.write('{}, {}\n'.format(comment, movie_code))
        pbar.update(1)
    f.close()

def crawl(opt : webdriver.ChromeOptions, input_q : Queue, output_q : Queue) :
    driver = webdriver.Chrome('chromedriver.exe', options=opt)
    while True :
        code, page = input_q.get()
        if code == -1 :
            break
        driver.get(page_url+str(code)+'&page='+str(page))
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        for i in range(10) :
            comment_tag = soup.select_one('span#_filtered_ment_{}'.format(i))
            if comment_tag is None :
                break
            comment = comment_tag.text.strip()
            output_q.put([comment, code])
    driver.close()

if __name__ == '__main__' :
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument("lang=ko_KR") 
    options.add_argument('log-level=3')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36 Edg/91.0.864.54")

    driver = webdriver.Chrome('chromedriver.exe', options=options)
    driver.implicitly_wait(1)
    
    PROC_N = 3
    TARGET_COMMENT = 200000

    movie_list = []
    for i in range(50) :
        driver.get(main_url + str(i + 1))
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        movie_page = soup.select('#old_content > ul > li')
        for m in movie_page :
            movie_link = m.a['href']
            movie_list.append(int(movie_link[movie_link.find('code=') + 5:]))

    total_cnt = 0
    movie_n = 0
    comment_n_list = []
    
    input_q = Queue()
    output_q = Queue()

    for movie in movie_list :
        if TARGET_COMMENT < total_cnt :
            break
        driver.get(page_url + str(movie))
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        comment_n = int(soup.select_one('div.score_total > strong > em').text.replace(',', ''))
        comment_n_list.append(comment_n)
        movie_n += 1
        total_cnt += comment_n
        print('Movie({:>6}) has {:>5} comments / Total {} movies {} comments'.format(movie, comment_n, movie_n, total_cnt))
        for j in range(ceil(comment_n / 10)) :
            input_q.put([movie, j + 1])
    driver.close()


    writer = Process(target=file_writer, args=(output_q, total_cnt,), daemon=True)
    writer.start()
    crawlers = []
    for i in range(PROC_N) :
        proc = Process(target=crawl, args=(options, input_q, output_q, ), daemon=True)
        proc.start()
        crawlers.append(proc)
            
    for i in range(PROC_N) :
        input_q.put([-1, 0])

    for proc in crawlers :
        proc.join()

    output_q.put(['', -1])
    writer.join()