import csv
import json
import re
from urllib.request import urlopen

import duckdb
import lxml.html
import pandas as pd
import requests

LETTERS = re.compile("[^a-zA-Z \n*]")


def scrape_goodreads(title, author):
    search_phrase = "+".join(title.split() + author.split())
    res = requests.get(
        f"https://www.goodreads.com/search?q={search_phrase}&ref=nav_sb_noss_l_17"
    )
    tree = lxml.html.fromstring(res.text)
    ratings = tree.xpath("//span[contains(text(),'rating')]/text()")[:5]
    ratings = [
        int(re.compile("(\d+,*\d*) rating").findall(r)[0].replace(",", ""))
        for r in ratings
    ]
    max_rate_index = ratings.index(max(ratings))
    search_res = [
        i.get("href")
        for i in tree.xpath("//a[contains(@href, '/book') and @class='bookTitle']")
        if i.get("href").endswith(f"&rank={max_rate_index+1}")
    ][0]
    res = requests.get(f"https://www.goodreads.com/{search_res}", timeout=(3.05, 27))
    tree = lxml.html.fromstring(res.content)
    genres = tree.xpath(
        "//span[@class='BookPageMetadataSection__genreButton']/a/span/text()"
    )
    pub_year = re.compile("\d{4}").findall(
        tree.xpath("//p[@data-testid='publicationInfo']/text()")[0]
    )[0]
    return genres, pub_year


def book_genres_aus_ca():
    conn = get_conn()
    for _, book in (
        conn.execute(
            """SELECT title, author FROM pg_books_ca
            /*  where subjects is null */"""
        )
        .fetchdf()
        .iterrows()
    ):
        try:
            genres, year = scrape_goodreads(book.title, LETTERS.sub("", book.author))
            print(book.title, book.author, genres, year)
        except:
            print(f"not found for {book.title}")


def book_subjects_pg():
    conn = get_conn()
    for _, book in (
        conn.execute("SELECT id  FROM pg_books where subjects is null")
        .fetchdf()
        .iterrows()
    ):
        res = requests.get(f"https://www.gutenberg.org/ebooks/{book.id}")
        tree = lxml.html.fromstring(res.text)
        subjects = [
            s.strip()
            for s in tree.xpath("//a[contains(@href, '/subject')]/text()")
            if "'" not in s
        ]
        if subjects:
            try:
                conn.execute(
                    f"""
            update pg_books set subjects={subjects}
            where id={book.id}
            """
                )
            except:
                print(f"error updating {subjects} for {book.id}")


def pg_books_metadata():
    conn = get_conn()
    reader = csv.reader(
        open("/Users/adiraz/Downloads/pg_catalog.csv")
    )  # gutenberg us catalog: https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv
    headers = next(reader)
    headers = {i: headers.index(i) for i in headers}
    for r in reader:
        if r[headers["Language"]] == "en":
            conn.execute(
                f"""INSERT INTO pg_books
                        (id, link, title, author)
                        VALUES ({r[headers['Text#']]},
                        'https://www.gutenberg.org/cache/epub/{r[headers['Text#']]}/pg{r[headers['Text#']]}.txt',
                        '{r[headers['Title']].replace("'", "''")}',
                        '{r[headers['Authors']].replace("'", "''")}')"""
            )


def get_start_and_end_lines(website_format, text_lines):
    if website_format == "pg":
        start_line = [
            l for l in text_lines if l.strip().startswith("***") and "start" in l
        ][0]
        end_line = [l for l in text_lines if l.strip().startswith("***") and "end" in l]
        end_line = end_line[0] if end_line else text_lines[-1]
        return start_line, end_line
    if website_format == "pg_ca":
        start_line = text_lines[
            text_lines.index(
                [i for i in text_lines if i.startswith("this ebook was produced by")][0]
            )
            + 3
        ]
        end_line = text_lines[-1]
        return start_line, end_line
    if website_format == "pg_aus":
        try:
            start_line = text_lines[
                text_lines.index(
                    [
                        i
                        for i in text_lines
                        if i.startswith("to contact project gutenberg")
                    ][0]
                )
                + 4
            ]
        except IndexError:
            start_line = text_lines[
                text_lines.index(
                    [
                        i
                        for i in text_lines
                        if "project gutenberg of australia license" in i
                    ][0]
                )
                + 7
            ]
        end_line = text_lines[-2]
        return start_line, end_line


def read_one_book(book, website_format):
    res = requests.get(book.link)
    if not res.ok:
        raise Exception(f"Error reading book utf, {book.title}")
    all_text = LETTERS.sub("", res.text)
    text_lines = all_text.lower().split("\n")
    text_lines = [i for i in text_lines if i]
    start_line, end_line = get_start_and_end_lines(
        website_format=website_format, text_lines=text_lines
    )
    text_lines = text_lines[
        text_lines.index(start_line) + 1 : text_lines.index(end_line) - 1
    ]
    book.length = len(text_lines)
    title_appearences = re.compile(f".*{book.title.lower()}.*").findall(
        "\n".join(text_lines)
    )
    title_appearences = [
        a
        for a in title_appearences
        if len(a.split()) > len(book.title.split())
        and text_lines.index(a) != book.length - 1
    ]
    book.title_appearences_locs = [text_lines.index(app) for app in title_appearences]
    book.title_appearences_relative = [
        i / book.length for i in book.title_appearences_locs
    ]
    book.title_appearences_count = len(title_appearences)
    return book


def read_books(table, website_format):
    conn = get_conn()
    for _, book in (
        conn.execute(
            f"SELECT id, link, title FROM {table} WHERE length IS NULL AND FAILED IS NULL"
        )
        .fetchdf()
        .iterrows()
    ):
        try:
            book = read_one_book(book, website_format=website_format)
            conn.execute(
                f"""UPDATE {table}
             SET length={book.length},
                 title_appearences_locs={book.title_appearences_locs},
                 title_appearences_relative={book.title_appearences_relative},
                 title_appearences_count={book.title_appearences_count} 
                 WHERE id={book.id}"""
            )
        except Exception as e:
            if "Error reading book utf" in repr(e):
                conn.execute(f"""UPDATE {table} SET failed=1 WHERE id={book.id}""")
            else:
                print(book.id, book.link)
                print(e)
                conn.execute(f"""UPDATE {table} SET failed=1 WHERE id={book.id}""")


def clean_pg_books():
    conn = get_conn()
    for _, book in (
        conn.execute("select * from pg_books where title like '%:%'")
        .fetchdf()
        .iterrows()
    ):
        conn.execute(
            f"""UPDATE pg_books
                     SET length=NULL,
                     title='{book.title.split(":")[0].replace("'","''")}'
                         WHERE id={book.id}"""
        )


def book_genres_csv():
    df = pd.read_csv(
        "/Users/adiraz/Desktop/datasets for book project/books_and_genres.csv"
    )  # this csv is from https://www.kaggle.com/datasets/michaelrussell4/10000-books-and-their-genres-standardized?resource=download
    df = df[["title", "genres"]]
    conn = get_conn()
    for _, i in df.iterrows():
        conn.execute(
            "INSERT INTO books_and_genres (title, genres)"
            f"VALUES ('{i['title']}', {list(eval(i['genres']))})"
        )


def book_genres_goodreads_csv():
    df = pd.read_csv(
        "/Users/adiraz/Desktop/datasets for book project/goodreads_data.csv"
    )  # this csv is from https://www.kaggle.com/datasets/ishikajohari/best-books-10k-multi-genre-data
    df = df[["Book", "Genres"]]
    conn = get_conn()
    for _, i in df.iterrows():
        title = i["Book"].replace("'", "''")
        try:
            conn.execute(
                "INSERT INTO books_and_genres (title, genres)"
                f"VALUES ('{title}', {list(eval(i['Genres']))})"
            )
        except duckdb.ConstraintException:
            pass


def project_gutenberg_aus_metadata():
    conn = get_conn()
    res = requests.get("http://www.gutenberg.net.au/catalogue.txt")
    if not res.ok:
        raise Exception("can't get catalog")
    for line in res.text.splitlines():
        if ",txt," not in line:
            continue
        link = (
            f"http://www.gutenberg.net.au/{line.split(',')[0]}/{line.split(',')[1]}.txt"
        )
        title = line.split(",")[-1]
        try:
            conn.execute(
                f"""INSERT INTO pg_books_aus
            (id, link, title, author)
            VALUES ({line.split(',')[1]},
            '{link}',
            '{title.replace("'", "''")}',
            '{' '.join(line.split(',')[3:-1]).replace("'", "''")}')"""
            )
        except duckdb.ConstraintException:
            pass
        except Exception as e:
            print(f"failed in line {line}, error {e}")


def project_gutenberg_ca_metadata():
    conn = get_conn()
    catalog = "http://gutenberg.ca/index.html#h2completecatalogue"
    res = requests.get(catalog)
    if not res.ok:
        raise Exception("failed getting canada catalog")
    tree = lxml.html.fromstring(res.text)
    text_links = tree.xpath("//a[text()='Text' and contains(@href, '.txt')]")
    for link in text_links:
        try:
            url = f"http://gutenberg.ca/{link.get('href')}"
            res = requests.get(url)
            if not res.ok:
                continue
            title = re.compile("Title: (.*)").findall(res.text)[0]
            id = re.compile("Project Gutenberg Canada ebook #(.*)").findall(res.text)[0]
            in_db = conn.execute(f"select * from pg_books_ca where id={id}")
            if in_db.fetchall():
                continue
            try:
                author = re.compile("Author: (.*)").findall(res.text)[0]
            except IndexError:
                continue
            try:
                year = re.compile("Date of first publication.*?(\d+)").findall(
                    res.text
                )[0]
            except:
                year = 0
            conn.execute(
                f"""INSERT INTO pg_books_ca
                    (id, link, title, author, year)
                    VALUES ({id},
                    '{url}',
                    '{title.replace("'", "''")}',
                    '{author.replace("'", "''")}',
                    {year})"""
            )
        except Exception as e:
            print(f"failed in {url}, error {e}")


def standardebooks_metadata():
    conn = get_conn()
    page_num = 1
    all_books = []
    while True:
        catalog = requests.get(
            f"https://standardebooks.org/ebooks?page={page_num}&per-page=48"
        )
        page_num += 1
        if "No ebooks matched your filters" in catalog.text:
            break
        tree = lxml.html.fromstring(catalog.content)
        text_links = set(
            i.get("href")
            for i in tree.xpath("//a[contains(@href, '/ebooks/')]")
            if i.get("href").startswith("/ebooks") and "page" not in i.get("href")
        )
        all_books.extend(text_links)
    for book in all_books:
        try:
            res = requests.get(f"https://standardebooks.org/{book}")
            tree = lxml.html.fromstring(res.content)
            title = tree.xpath("//h1")[0].text
            author = tree.xpath("//h2/a/span")[0].text
            tags = ", ".join([i.text for i in tree.xpath("//ul[@class='tags']/li/a")])
            single_page_link = f"https://standardebooks.org{book}/text/single-page"
            conn.execute(
                f"""INSERT INTO standard_ebooks
                                    (link, title, author, tags)
                                    VALUES (
                                    '{single_page_link}',
                                    '{title}',
                                    '{author}',
                                    '{tags}')"""
            )
        except duckdb.ConstraintException:
            pass
        except Exception as e:
            print(f"error in https://standardebooks.org/{book}")


def get_conn():
    return duckdb.connect(database="books.duckdb", read_only=False)


if __name__ == "__main__":
    pass
