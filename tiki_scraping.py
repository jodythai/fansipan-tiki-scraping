import datetime
import requests
from bs4 import BeautifulSoup
import db_init as db

TIKI_URL = 'https://tiki.vn'


def parse(url):
    """Retrive and parse HTML code of a url
    """
    # Get the HTML document
    try:
        res = requests.get(url).text
    except Exception as err:
        print('ERROR: Get URL fail: {}'.format(err))

    return BeautifulSoup(res, features="lxml")

def is_category_existed(url):
    """
        This function will check if a category is existed in the db or not
        by checking the category url. 
        If it is existed, return True, else False
    """
    
    existed_category = db.execute_query("SELECT 1 FROM categories WHERE url = '" + url + "'")
    
    # if the current category is not in the database
    if existed_category:
        return True
    else:
        return False

def add_sub_categories():

    categories = db.execute_query("SELECT category_id, url, weight FROM categories \
                                    WHERE parent IS NULL;")

    for item in categories:
        print('INFO: Get sub-categories of {}'.format(item[1]))
        total_products_sub = 0
        parent_id = item[0]
        weight = item[2]

        # parse the root category's html
        s = parse(item[1])

        # Find all sub category of the current root category
        sub_categories = s.findAll(
            'div', {'class': 'list-group-item is-child'})
        
        # loop through each sub category
        for sub in sub_categories:
            
            url = TIKI_URL + sub.a['href']
            
            # if the current sub category is not in the database, then insert it
            if is_category_existed(url) == False:
                index = sub.a.text.rindex('(')
                name = sub.a.text[:index].strip()
                count = int(sub.a.text.strip()[index+1:-1])
                created_on = datetime.datetime.now()

                print(count)
                # add the current sub category total products number to the total products number
                total_products_sub += count

                # insert sub category into db
                db.insert_row((name, url, parent_id, weight, count, created_on), 'categories')
        
        # After inserting all sub categories, update the total number of products
        # to the root category
        query = "UPDATE categories SET count = {} WHERE category_id = {}".format(total_products_sub, parent_id)
        db.update_query(query)

def get_categories():
    """Find all URLs of root categories on Tiki.vn"""
    print("INFO: Get root (parent) categories")

    """
    Define weight for each root category.
    we use this number to decide on how many products we will scrape 
    from each sub category of each root category. 
    The formula is: 
    total scraping products = (root category total products * weight) / number of sub category
    """
    weight_list = [1, 
                    0.3,
                    0.005,
                    0.1,
                    0.1,
                    0.1,
                    0.01,
                    0.05,
                    0.03,
                    0.03,
                    0.005,
                    0.1,
                    0.1,
                    0.01,
                    0.01,
                    0.2]

    s = parse(TIKI_URL)
    created_on = datetime.datetime.now()

    for index, item in enumerate(s.find_all('a', class_='MenuItem__MenuLink-tii3xq-1 efuIbv')):
        url = item.get('href')
        category = item.find('span', class_='text').text

        # In case the list of Tiki's root categories is changed,
        # we will add a default number for weight to keep this function 
        # from being interupted due to array item matching error
        weight = weight_list[index] if index < len(weight_list) else 1

        db.insert_row((category, url, None, weight, 0, created_on), 'categories')

def scraping_products_on_page(category_id, url):
    """Crawl all products  on a page and save into DB
    """
    s = parse(url)

    list_products = []

    # Find all tags <div class='product-item'> and store them in 'prodct_items' list, each tag represent a product
    product_items = s.findAll('div', {'class': 'product-item'})

    # If the tag list is empty (i.e. the page doesn't have any product), return an empty list.
    if len(product_items) == 0:
        return list_products
    else:
        # Iterate through all product and store the product information in the 'row' list
        for i in range(len(product_items)):
            created_on = datetime.datetime.now()
            tiki_now = True if product_items[i].find(
                'i', {'class': 'tikicon icon-tikinow'}) else False
            if product_items[i].find('span', {'class': 'rating-content'}):
                avg_rating = int(str(product_items[i].find(
                    'span', {'class': 'rating-content'}).span['style']).split(':')[1][:-1])
            else:
                avg_rating = None

            total_ratings = 0
            if len(product_items[i].select('.review-wrap .review')) > 0:
                total_ratings = product_items[i].select_one(
                    '.review-wrap .review').string
                total_ratings = total_ratings[total_ratings.find(
                    '(')+1:total_ratings.find(' ')]

            row = [
                category_id,
                product_items[i]['data-id'],
                product_items[i]['data-seller-product-id'],
                product_items[i]['data-title'],
                product_items[i]['data-price'],
                product_items[i]['data-brand'],
                avg_rating,
                total_ratings,
                product_items[i].a['href'],
                product_items[i].find(
                    'img', {'class': 'product-image img-responsive'})['src'],
                created_on,
                tiki_now
            ]

            # Add the product information of each product into 'results' list
            list_products.append(row)

    return list_products

def scrape_all():
    """
    This function will:
    1. Get the list of sub categories from the db
    2. For each sub category, it will scrape the products 
        until the total scraped products reach the maximum scraping products
        we calculate with weight number. At that point, it will change to another sub category.
    """

    print('INFO scrape_all(): Start craping')
    
    results = []
    queue = []

    # Get all sub category links
    categories = db.execute_query("SELECT category_id, url, weight, count \
                                    FROM categories WHERE parent IS NOT NULL;")
    
    # Compute the list of sub categories
    for cat in categories:
        url = cat[1]
        cat_id = cat[0]
        weight = cat[2]
        count = cat[3]

        # Set the maximum number of products will be scraped for each sub category
        max_products = round(weight*count)

        queue.append((cat_id, url, max_products))

    while queue:
        url = queue[-1][1]
        cat_id = queue[-1][0]
        max_product = queue[-1][2]
        queue.pop()
        new_rows = scraping_products_on_page(cat_id, url)

        print(len(new_rows))

        if new_rows:
            results += new_rows
            
            # Insert products to database
            for product in new_rows:
                db.insert_row(product, "products")

            total_product = len(new_rows)
            page = int(url.split('&page=')[1]) + 1 if len(url.split('&page=')) > 1 else 2
            new_url = url.split('&page=')[0] + '&page=' + str(page) 
            
            max_product -= total_product
            if max_product >= 0:
                queue.append((cat_id, new_url, max_product))
                print("{} {} {}".format(cat_id, new_url, max_product))

    # Return the final list of all products
    return results

db.create_tables()
get_categories()
add_sub_categories()
#scrape_all()
