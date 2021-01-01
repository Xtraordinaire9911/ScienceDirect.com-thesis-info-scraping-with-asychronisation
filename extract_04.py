import pyppeteer
import asyncio
import nest_asyncio
import requests
from lxml import etree
import time
import pandas as pd
import numpy as np
import warnings


class Extract(object):
    def __init__(self, query='', limit_num_pages=240, limit_num_theses=6000, sleep_search=2, sleep_extract=3):
        self.time_start = time.time()
        self.limit_num_pages = limit_num_pages
        self.limit_num_theses = limit_num_theses
        self.sleep_search = sleep_search
        self.sleep_extract = sleep_extract

        self.url_main = 'https://www.sciencedirect.com'
        self.headers = {
            'Connection': 'close',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
            # 'proxy': []
        }
        if len(query) > 0:
            self.l_url_search = ['https://www.sciencedirect.com/search?qs={}&articleTypes=FLA&offset={}&show=100'
                                     .format(query, str(100 * i)) for i in range(60)]
        else:
            self.l_url_search = ['https://www.sciencedirect.com/search?articleTypes=FLA&offset={}&show=100'
                                     .format(str(100 * i)) for i in range(60)]
        self.l_url_extract_try = ['https://www.sciencedirect.com/science/article/pii/S0039606020305171',
                                  'https://www.sciencedirect.com/science/article/pii/S000165192030145X',
                                  'https://www.sciencedirect.com/science/article/pii/S0009739X20302682',
                                  'https://www.sciencedirect.com/science/article/pii/S0034361720302630',
                                  'https://www.sciencedirect.com/science/article/pii/S0038110120303518']
        # hook  禁用 防止监测webdriver
        pyppeteer.launcher.DEFAULT_ARGS.remove("--enable-automation")

        self.l_url_extract = []
        self.df_url_extract = pd.DataFrame([{'论文详细信息页网址：', ''}])
        self.l_dict_info = []
        self.df_info = pd.DataFrame([{'title': '',
                                      'author_1': '',
                                      'author_1_degrees': '',
                                      'author_1_institution': '',
                                      'author_1_country': '',
                                      'author_2': '',
                                      'author_2_degrees': '',
                                      'author_2_institution': '',
                                      'author_2_country': '',
                                      'authors_rest': [],
                                      'authors_rest_degrees': [],
                                      'authors_rest_institutions': [],
                                      'authors_rest_countries': [],
                                      'abstract': ''
                                      }])
        # self.loop = asyncio.get_event_loop()


    async def parse_search_result(self, url):
        browser = await pyppeteer.launch(headless=False)
        page = await browser.newPage()
        print('\n\n正在打开搜索页面...', time.time() - self.time_start)
        await page.goto(url, verify=False)

        k = int(url.split('=')[-2].split('&')[0]) // 100 + 1
        print('正在解析第{}张搜索页...'.format(str(k)), time.time() - self.time_start)
        print('当前页面url为：', url)
        page_text = await page.content()
        if page_text == '' or page_text == [] or page_text == None:
            print('出错：获取到的页面为空值', time.time() - self.time_start)
        # print(page_text)
        l_li = await page.xpath('//li[@class="ResultItem col-xs-24 push-m"]')
        print('l_li:', l_li)
        # print('l_li[-1]的url：', await l_li[-1].xpath('.//a/@href')[0])
        print('num_li:', len(l_li))
        # if len(l_li) == 0:
        #     print(page_text)
        # print(page.xpath('//*[@id="srp-results-list"]/ol/li[{}]/div/div[2]/h2/span/a/@href'.format(len(l_li)-1)))

        print('正在汇总论文详细信息页网址...', time.time() - self.time_start)
        l_url_extract = []
        for li in l_li:
            handle_elem = await li.xpath('.//a')
            await handle_elem[0].click()
            url_extract = await (await handle_elem[0].getProperty('href')).jsonValue()

            if url_extract != '':
                l_url_extract.append({'论文详细信息页网址：', url_extract})
        # '//*[@id="srp-results-list"]/ol/li[1]/div/div[2]/h2/span/a'
        # '//*[@id="srp-results-list"]/ol/li[{}]/div/div[2]/h2/span/a/@href'
        await page.close()
        await browser.close()

        self.l_url_extract += l_url_extract
        print('len(l_url_extract)', len(self.l_url_extract))
        self.df_url_extract = pd.concat(self.df_url_extract, pd.DataFrame(l_url_extract))
        return page, self.l_url_extract


    async def parse_extract_info(self, url):
        browser = await pyppeteer.launch(headless=False)
        page = await browser.newPage()
        await page.goto(url, verify=False)
        page_text = await  page.content()

        dict_info = {}
        # title = await page.waitForXPath('//h1[@id="screen-reader-main-title"]/span/text()')
        handle_elem = await page.xpath('//h1[@id="screen-reader-main-title"]/span')
        await handle_elem[0].click()
        title = await (await handle_elem[0].getProperty('textContent')).jsonValue()
        dict_info['title'] = title
        print('\n\n正在解析论文《{}》的详细信息页...'.format(title), time.time() - self.time_start)


        await page.close()
        await browser.close()


    def main(self):
        nest_asyncio.apply()
        warnings.simplefilter('ignore', ResourceWarning)
        print('开始爬取工作...', time.time() - self.time_start)
        if self.limit_num_pages > 1:
            num_pages = int(etree.HTML(requests.get(self.l_url_search[0], headers=self.headers).text)
                            .xpath('.//*[@id="srp-pagination"]/li/text()')[0].split('of')[-1].strip())
        else:
            num_pages = 1
        print('共有{}页搜索结果'.format(str(min(num_pages, self.limit_num_pages))))
        num_batch_search = (min(self.limit_num_pages, num_pages) - 1) // 20 + 1
        for i in range(num_batch_search):
            # await asyncio.sleep(self.sleep_search)
            time.sleep(self.sleep_search)
            print('finish sleeping, start searching', time.time() - self.time_start)
            l_task_search = []
            for url in self.l_url_search[20 * i : min(20 * (i + 1), num_pages, self.limit_num_pages)]:
                # await asyncio.sleep(self.sleep_search)
                task_search = asyncio.ensure_future(self.parse_search_result(url))
                # task_search.add_done_callback(self.parse_extract_info)
                l_task_search.append(task_search)
            print('there are {} search tasks in total in batch_{}'.format(str(len(l_task_search)), str(i + 1)))
            loop_search = asyncio.get_event_loop()
            loop_search.run_until_complete(asyncio.wait(l_task_search))
            self.df_url_extract.to_csv('./url_extract_restore_{}.csv'.format(str(i + 1)), encoding='utf-8')
        print('搜索完毕！共有{}条搜索结果'.format(str(len(self.l_url_extract))), time.time() - self.time_start)
        self.df_url_extract = self.df_url_extract.reset_index().drop(0, axis=0).T
        self.df_url_extract.to_csv('./url_extract.csv', encoding='utf-8')
        print('self.df_url_extract:', self.df_url_extract)
        print('shape of self.df_url_extract: {}'.format(self.df_url_extract.shape))

        num_theses = min(self.limit_num_theses, len(self.l_url_extract))
        num_batch_extract = (num_theses - 1) // 20 + 1
        for i in range(num_batch_extract):
            # await asyncio.sleep(self.sleep_search)
            time.sleep(self.sleep_extract)
            print('finish sleeping, start extracting', time.time() - self.time_start)
            l_task_extract = []
            for url in self.l_url_extract[20 * i: min(20 * (i + 1), num_theses)]:
                # await asyncio.sleep(self.sleep_extract)
                task_extract = asyncio.ensure_future(self.parse_extract_info(url))
                l_task_extract.append(task_extract)
            print('there are {} extract tasks in total in batch_{}'.format(str(len(l_task_extract)), str(i + 1)))
            loop_search = asyncio.get_event_loop()
            loop_search.run_until_complete(asyncio.wait(l_task_extract))
        print('爬取完毕！共采集到并解析了{}篇论文的详细信息'.format(num_theses))
        # self.df_in.to_csv('./url_extract_restore_{}.csv'.format(str(i + 1)), encoding='utf-8')
        # print('搜索完毕！共有{}条搜索结果'.format(str(len(self.l_url_extract))), time.time() - self.time_start)
        # self.df_url_extract.to_csv('./url_extract.csv', encoding='utf-8')
        # print(self.df_url_extract)
        # print('shape of self.df_url_extract: {}'.format(self.df_url_extract.shape))

        #     self.df_url_extract.to_csv('./url_extract_restore_{}.csv'.format(str(i + 1)), encoding='utf-8')
        # print('搜索完毕！共有{}条搜索结果'.format(str(len(self.l_url_extract))), time.time() - self.time_start)



if __name__ == '__main__':
    ex = Extract(limit_num_pages=2)
    ex.main()



