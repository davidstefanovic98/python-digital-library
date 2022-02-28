import json
import re
from itertools import islice
from flask import Flask, request

import requests as req
import pandas as pd


def are_columns_null(row):
    return pd.isna(row["Naslov"]) and \
           pd.isna(row["Godina"]) and \
           pd.isna(row["Opis"])


def are_not_float(row):
    return not isinstance(row["Opis"], float) and \
           not isinstance(row["Naslov"], float) and \
           not isinstance(row["Godina"], float) and \
           not isinstance(row["Link do knjige"], float)


subcategories = set()

test_for_subcategories_list = list()

link_to_books = set()

categories = {

}

tags = set()

authors = set()
publishers = set()

data = {

}


def format_sql(id, row):
    title = row["Naslov"].replace("'", "\\'")
    description = row["Opis"].replace("'", "\\'")
    issue_year = row["Godina"]
    return f"insert into digital_lib_book (digital_lib_book_id, title, description, issue_year) values ({id},'{title}','{description}',{issue_year});\n"


def author_format_sql(id, author):
    first_name = author.split(" ")[0].replace("'", "\\'")
    last_name = " ".join(author.split(" ")[1:]).replace("'", "\\'")
    return f"insert into digital_lib_author (digital_lib_author_id, first_name, last_name) values ({id},'{first_name}','{last_name}');\n"


def tag_format_sql(id, tag):
    tag_name = tag.replace("'", "\\`")
    return f"insert into digital_lib_tag (digital_lib_tag_id, name) values ({id},'{tag_name}');\n"


def publisher_format_sql(id, publisher):
    publisher_name = publisher.replace("'", "\\`")
    return f"insert into digital_lib_publisher (digital_lib_publisher_id, name) values ({id},'{publisher_name}');\n"


def book_author_format_sql(book_id, author_id):
    return f"insert into digital_lib_book_author (digital_lib_book_fk, digital_lib_author_fk) values ({book_id},{author_id});\n"


def book_tag_format_sql(book_id, tag_id):
    return f"insert into digital_lib_book_tag (digital_lib_book_fk, digital_lib_tag_fk) values ({book_id},{tag_id});\n"


def book_publisher_format_sql(book_id, publisher_id):
    return f"insert into digital_lib_book_publisher (digital_lib_book_fk, digital_lib_publisher_fk) values ({book_id},{publisher_id});\n"


def categories_format_sql(id, category):
    return f"insert into digital_lib_category (digital_lib_category_id, name) values ({id},'{category}');\n"


def book_category_format_sql(book_id, category_id):
    return f"insert into digital_lib_category_book (digital_lib_book_fk, digital_lib_category_fk) values ({book_id},{category_id});\n"


def subcategories_format_sql(id, parent_category_id, child_category_id):
    return f"insert into digital_lib_subcategory (digital_lib_subcategory_id, parent_digital_lib_category_fk, child_digital_lib_category_fk) values ({id},{parent_category_id},{child_category_id});\n"


def digital_lib_file_format_sql(id, book_id, file_name, alfresco_id, alfresco_link):
    return f"insert into digital_lib_book_file (digital_lib_book_file_id, digital_lib_book_fk, file_name, alfresco_id, alfresco_link) values ({id},{book_id},'{file_name}', '{alfresco_id}', '{alfresco_link}');\n"


def main():
    split_pattern = re.compile(r"\s*([,\n·]+|\s+y\s+|\s+and\s+|\s+&\s+)\s*")
    split_pattern_for_publisher = re.compile(r"\s*([,\n·]+|\s+y\s+|\s+and\s+)\s*")
    clean_pattern = re.compile(
        r"(\(?\s*-?\s*\b[eE]ds?(itors?:?)?\.?\)?|\s*\(Coords.\)|\s+\(view affiliations\)|\s*adapt by\s+|\s*By\s+|\s*and\s+|\t)")
    clean_pattern_for_category = re.compile(
        r"(\(?\s*-?\s*\b[eE]ds?(itors?:?)?\.?\)?|\s*\(Coords.\)|\s+\(view affiliations\)|\s*adapt by\s+|\s*By\s+|\t)")
    clean_pattern_for_subcategory = re.compile(
        r"(\?|\s*\(Coords.\)|\s+\(view affiliations\)|\s*adapt by\s+|\s*By\s+|\t)")

    sql = ""
    df = pd.read_csv("data.csv", header=1)
    iterator = islice(df.iterrows(), 0, None)

    category = None
    subcategory = None

    for i, row in iterator:
        if pd.isna(row["Kategorija"]):
            row["Kategorija"] = category
        else:
            category = row["Kategorija"]

        if pd.isna(row["Potkategorija"]):
            row["Potkategorija"] = subcategory
        else:
            subcategory = row["Potkategorija"]

        if not are_columns_null(row) and are_not_float(row):
            strindex = str(len(data.values()) + 1)
            data[strindex] = {
                "Id": strindex,
                "Naslov": row["Naslov"] if isinstance(row["Naslov"], float) else re.sub(r"(\r?\n)+", " ",
                                                                                        row["Naslov"].strip(" \n\t\r")).replace("'", "\\`"),
                "Godina": row["Godina"],
                "Opis": row["Opis"] if isinstance(row["Opis"], float) else re.sub(r"(\r?\n)+", " ",
                                                                                  row["Opis"].strip(" \n\t\r")).replace("'", "\\`"),
                "Autor": [],
                "Izdavac": [],
                "Tagovi": [],
                "Kategorija": [],
                "Potkategorija": [],
                "Link do knjige": []
            }
            for tag in split_pattern.split(str(row["Tagovi"])):
                tag = clean_pattern.sub("", tag.strip(" \n\t\r"))
                if tag != "," and tag != "":
                    data[strindex]["Tagovi"].append(tag)
                    tags.add(tag)

            if not pd.isna(row["Autor"]):
                for author in split_pattern.split(str(row["Autor"])):
                    author = clean_pattern.sub("", author.strip(" \n\t\r"))
                    if author != "" and author != "y" and author != "," and author != "and":
                        data[strindex]["Autor"].append(author)
                        authors.add(author)

            for publisher in split_pattern_for_publisher.split(str(row["Izdavac"])):
                publisher = clean_pattern.sub("", publisher.strip(" \n\t\r"))
                if publisher != "" and publisher != ",":
                    data[strindex]["Izdavac"].append(publisher)
                    publishers.add(publisher)

            if not pd.isna(row["Kategorija"]):
                category = clean_pattern_for_category.sub("", category.strip(" \n\t\r"))
                data[strindex]["Kategorija"].append(category)
                categories[row["Kategorija"]] = category

            if not pd.isna(row["Potkategorija"]):
                subcategory = clean_pattern_for_subcategory.sub("", subcategory.strip(" \n\t\r"))
                if subcategory != "" and subcategory != ",":
                    data[strindex]["Potkategorija"].append(subcategory)
                    s = {"subcategory": subcategory, "category": category}
                    subcategories.add(json.dumps(s))

            if not pd.isna(row["Link do knjige"]):
                for link in split_pattern.split(str(row["Link do knjige"])):
                    link = link.strip(" \n\t\r")
                    data[strindex]["Link do knjige"].append(link)
                    link_to_books.add(link)

    for author in authors:
        sql += (author_format_sql(list(authors).index(author) + 1, author))

    for tag in tags:
        sql += (tag_format_sql(list(tags).index(tag) + 1, tag))

    for publisher in publishers:
        sql += (publisher_format_sql(list(publishers).index(publisher) + 1, publisher))

    for i, category in enumerate(categories):
        categories[category] = i

    total = len(categories.keys())
    for i, subcategory in list(enumerate(subcategories)):
        categories[eval(subcategory)["subcategory"]] = {"id": total + i + 1,
                                                        "fk": categories[eval(subcategory)["category"]]}

    temp_for_subcategories_list = list(categories.values())
    temp_for_subcategories_list_items = list(categories.items())

    for i, subcategory in temp_for_subcategories_list_items[0:6]:
        sql += categories_format_sql(categories.get(i) + 1, i)

    for i, subcategory in temp_for_subcategories_list_items[6:len(temp_for_subcategories_list_items) - 1]:
        sql += categories_format_sql(subcategory["id"] + 1, i)

    for i, subcategory in list(enumerate(temp_for_subcategories_list[6:len(temp_for_subcategories_list) - 1])):
        sql += subcategories_format_sql(int(i) + 1, subcategory["fk"] + 1, subcategory["id"] + 1)

    for i, row in data.items():
        if not are_columns_null(row):
            sql += format_sql(int(i), row)
            for author in row["Autor"]:
                sql += (book_author_format_sql(int(i), list(authors).index(author)))
            for tag in row["Tagovi"]:
                sql += (book_tag_format_sql(int(i), list(tags).index(tag)))
            for publisher in row["Izdavac"]:
                sql += (book_publisher_format_sql(int(i), list(publishers).index(publisher)))
            for category in row["Kategorija"]:
                sql += (book_category_format_sql(int(i), list(categories).index(category) + 1))
            for subcategory in row["Potkategorija"]:
                sql += (book_category_format_sql(int(i), categories.get(f'{subcategory}')["id"] + 1))

    with open("lib.sql", "w", encoding="utf-8") as file:
        file.write(sql)



if __name__ == '__main__':
    main()
