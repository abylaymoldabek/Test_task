# -*- coding: utf-8 -*-
import logging
import time
from datetime import datetime, timedelta
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
import pymysql

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='sample.log', level=logging.ERROR, format=FORMAT)


def start(connect):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(
        executable_path='/home/abylay/PycharmProjects/python/chromedriver',
        options=chrome_options
    )

    driver.get('https://tengrinews.kz/')
    # menu = driver.find_element_by_class_name('tn-all_news')
    # sub_menu = driver.find_element_by_css_selector('.tn-tape-title')
    # action = ActionChains(driver)
    # elements = WebDriverWait(driver, 10).until(
    #     EC.presence_of_all_elements_located((By.CLASS_NAME, "tn-tape-item"))
    # )
    # for element in elements:
    #     print(element.text)
    #     element.click()
    # import pdb
    # pdb.set_trace()
    parse = parse_main_page(driver.page_source, connect)
    driver.close()
    return parse


def parse_main_page(content, connect):
    soup = BeautifulSoup(content, 'html.parser')
    money_list = soup.find('div', {'class': 'tn-all_news tn-active'})
    div_list = money_list.findAll('div')
    lst = []

    for div in div_list:
        a_list = div.find_all('a', href=True)
        for a in a_list:
            url = a['href']
            print(datetime.now(), url)
            lst.append(parse_detail_page(url=url, connect=connect))
    return lst


def parse_detail_page(url, connect):
    base_url = "https://tengrinews.kz"
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(
        executable_path='/home/abylay/PycharmProjects/python/chromedriver',
        options=chrome_options
    )
    if url[:4] == 'http':
        pass
    else:
        url = base_url + url
        driver.get(url=url)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        title = soup.find('h1', {'class': 'tn-content-title'})
        find_time = soup.find('time', {'class': 'tn-visible@t'}).text
        if 'сегодня' in find_time:
            today = find_time.replace('сегодня', str(datetime.now().date()))
            publish_tm = datetime.strptime(today, '%Y-%m-%d, %H:%M')
        elif 'вчера' in find_time:
            yesterday = find_time.replace('вчера', str(datetime.now().date() - timedelta(1)))
            publish_tm = datetime.strptime(yesterday, '%Y-%m-%d, %H:%M')
        item_id = insert_items_to_db(
            connection=connect,
            link=url,
            title=title.contents[0].strip(),
            content=soup.find("div", {"class": "tn-news-content"}).text.strip(),
            publish_date=publish_tm.date(),
            publish_datetime=publish_tm.time(),
            parse_date=datetime.now()
        )
        print(item_id['id'])
        time.sleep(10)
        element = driver.find_element_by_xpath('//span[contains(text(), "Показать комментарии")]')
        driver.execute_script(f"arguments[0].click();", element)
        comments_list = driver.find_elements_by_class_name('tn-comment-item-content')
        # print(comments_list)
        for comment_element in comments_list:
            parse_datetime = datetime.now()
            author = str(comment_element.find_element_by_class_name('tn-user-name').text)
            date = comment_element.find_element_by_tag_name('time').text
            comment = comment_element.find_element_by_class_name('tn-comment-item-content-text').text
            insert_comment_to_db(connection=connect, item_id=item_id['id'],
                                 author=author,
                                 date=date,
                                 comment=comment,
                                 parse_date=parse_datetime
                                 )
            # import pdb
            # pdb.set_trace()
        driver.close()


def connection_with_db():
    connection = pymysql.connect(
        host="localhost",
        user="root",
        passwd="qwerty123",
        db="Test",
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection


def insert_items_to_db(connection, link, title, content, publish_date, publish_datetime, parse_date):
    try:
        with connection.cursor() as cursor:
            query = f"INSERT INTO items (link, title, content, publish_date,publish_datetime, " \
                    f"parse_date) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (link, title.encode('utf-8').decode('utf-8'), content, publish_date,
                                   str(publish_datetime)[:5].replace(':', ''), parse_date))
            print("ok")
        connection.commit()
        with connection.cursor() as cursor:
            sql = "SELECT `id` FROM `items` WHERE `title`=%s"
            cursor.execute(sql, (title,))
            result = cursor.fetchone()
    except Exception as e:
        logging.error("Exception")
        print(e)
    return result


def insert_comment_to_db(connection, item_id, author, date, comment, parse_date):
    try:
        with connection.cursor() as cursor:
            query = f"INSERT INTO comments (item_id, author, date, comment,parse_date) " \
                    f"VALUES (%s, %s, %s, %s, %s)"
            import pdb
            pdb.set_trace()
            cursor.execute(query, (item_id, author, date, comment, parse_date))
        print("ok!")
        connection.commit()
    except Exception as e:
        logging.error("Exception")
        print(e)


if __name__ == '__main__':
    connection = connection_with_db()
    start(connection)
