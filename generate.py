import os
import textwrap
import requests
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Spacer, Paragraph
from reportlab.platypus.flowables import Image
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet


def extract_data_to_df():

    # Connect to your PostgreSQL database
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()

    # Execute SQL query
    today_date = datetime.today().date().strftime("%Y-%m-%d")
    sql = """
            SELECT rank, post_title, media_url, ups_num, comments_num, 
            comment_author_1, comment_score_1, comment_1,
            comment_author_2, comment_score_2, comment_2,
            comment_author_3, comment_score_3, comment_3
            FROM top_posts
            WHERE rank <= 3 AND extract_date = '{}'
          """.format(today_date)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()

    # Convert rows to a pandas DataFrame
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    comment_columns = ['comment_1', 'comment_2', 'comment_3']
    for column in comment_columns:
        df[column] = df[column].apply(lambda x: '\n'.join(textwrap.wrap(x, width=110)))

    return df


def create_pdf_with_tables(df, file_name):
    doc = SimpleDocTemplate(file_name, pagesize=landscape(letter), topMargin=10, leftMargin=80)
    elements = []

    df1_style = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.black),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey)
    ]

    df2_style = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightblue),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]

    for index, row in df.iterrows():
        df_row_index = df.loc[[index], ["rank", "post_title", "media_url", "ups_num", "comments_num"]]
        df_row_index.columns = ["Rank", "Post", "URL", "No. of Votes", "No. of Comments"]
        df_row_index = df_row_index.T.reset_index()

        df_comment_index = df.loc[[index], ["comment_author_1", "comment_score_1", "comment_1",
                                            "comment_author_2", "comment_score_2", "comment_2",
                                            "comment_author_3", "comment_score_3", "comment_3"]]
        df_comment_index.columns = ["Author 1", "Score 1", "Comment 1",
                                    "Author 2", "Score 2", "Comment 2",
                                    "Author 3", "Score 3", "Comment 3"]
        df_comment_index = df_comment_index.T.reset_index()

        # Append df_row_table table with df1 styling
        df_row_table_data = df_row_index.values.tolist()
        for i in range(len(df_row_table_data)):
            if i == 2:  # Check if it's the URL column
                media_url = df_row_table_data[i][1]
                image_width = inch * 2
                image_height = inch * 1.5
                try:
                    img = Image(media_url, width=image_width, height=image_height)
                    df_row_table_data[i][1] = img
                except:
                    # If there's an error loading the image, convert the path to a clickable hyperlink
                    df_row_table_data[i][1] = Paragraph('<a href="%s">%s</a>' % (media_url, media_url),
                                                        getSampleStyleSheet()["BodyText"])

        df_row_table = Table(df_row_table_data, colWidths=[100, 570])
        df_row_table.setStyle(TableStyle(df1_style))
        elements.append(df_row_table)
        elements.append(Spacer(1, 30))

        # Append df_comment_table table with df2 styling
        df_comment_table = Table(df_comment_index.values.tolist(), colWidths=[100, 570])
        df_comment_table.setStyle(TableStyle(df2_style))
        elements.append(df_comment_table)
        elements.append(Spacer(1, 60))

    doc.build(elements)


if __name__ == "__main__":
    load_dotenv()

    url = os.getenv("API_URL")
    payload = {'auth': os.getenv("API_AUTH_KEY"), 'sr': 'memes', 'top': '20'}
    response = requests.post(url, data=payload)

    if response.status_code == 200:
        create_pdf_with_tables(extract_data_to_df(), 'telegram_report2.pdf')
