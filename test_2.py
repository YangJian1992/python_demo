import pytesseract
from PIL import Image
import PIL
from selenium import webdriver
import os
import time

#这是一个测试
if __name__ == '__main__':
    # data = DataFrame([[3, 4, 5], [5, 6, 7], [5, 5, 5]], columns=['a', 'b', 'c'])
    # a = [('john', 'A', 15), ('jane', 'C', 12), ('dave', 'B', 10)]
    # b = ['a', '2', 2, 4, 5, '2', 'b', 4, 7, 'a', 5, 'd', 'a', 'z']
    # f = DataFrame(np.array(data), index=data['a'].values, columns=['a', 'b', 'c'])
    #
    # print(data)
    # print(data.set_index('b',  drop=False))
    os.environ['MOZ_HEADLESS'] = '1'
    options = webdriver.FirefoxOptions()
    driver = webdriver.Firefox(firefox_options=options)
    print('已经登陆豆瓣')
    driver.get('https://www.douban.com/')
    print('已经登陆豆瓣')
    driver.find_element_by_name('form_email').send_keys('邮箱')
    driver.find_element_by_name('form_password').send_keys('密码')
    print("已经输入用户名和密码")
    driver.find_element_by_xpath('//input[@class="bn-submit"]').click()
    print("已点击，请稍候")
    time.sleep(2)
    driver.save_screenshot("douban.png")
    driver.quit()
    # content = driver.find_elements_by_xpath('//a')
    # print(content)
    # a = [item.text for item in content]
    # b = [item.get_property('href') for item in content]
    # c = zip(a,b)
    # [print(item) for item in c]
    # title = driver.find_element_by_id('wrapper').text
    # search_text = driver.find_element_by_id('kw')

    # search_text.send_keys('selenium')
    #print(driver.page_source)



    # image = Image.open('1.jpg')
    # #pytesseract.tesseract_cmd = r'C:/Program Files (x86)/Tesseract-OCR/tesseract.exe'
    # code = pytesseract.image_to_string(image)
    # print(code, type(code))