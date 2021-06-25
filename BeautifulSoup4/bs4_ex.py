from bs4 import BeautifulSoup

f = open('example.html')
html = f.read(1000)

soup = BeautifulSoup(html, 'html.parser')

a = soup.select_one('body > ul')
b = soup.body.ul

print(a.select('li'))
print(a.li)

print(soup.body.h1.a['href'])