import sys, time, re
from PyQt6.QtWidgets import *
import psycopg2
from configparser import ConfigParser
from pyqtgraph import PlotWidget, plot
import pyqtgraph as pg
from urllib.request import urlopen
from pymongo import MongoClient



class MainWindow(QMainWindow):
    def __init__(self):  # DONE 
        super().__init__()
        self.conn = None
        self.cur = None
        self.chooseDatabase = 0

        self.setWindowTitle("Prosta aplikacja z PyQt6")
        self.setGeometry(100, 100, 600, 300)

        # Tworzenie głównego widżetu
        self.central_widget = QTabWidget()
        self.setCentralWidget(self.central_widget)

        # Dodawanie zakładek
        self.connect_db_tab = QWidget()
        self.basic_tests_tab = QWidget()
        self.statistics_tab = QWidget()
        self.charts_tab = QWidget()
        self.custom_query_tab = QWidget()
        self.web_scraping_tab = QWidget()

        self.central_widget.addTab(self.connect_db_tab, "Database")
        self.central_widget.addTab(self.basic_tests_tab, "Basic Tests")
        self.central_widget.addTab(self.statistics_tab, "Statistics")
        self.central_widget.addTab(self.charts_tab, "Charts")
        self.central_widget.addTab(self.custom_query_tab, "Custom Query")
        self.central_widget.addTab(self.web_scraping_tab, "Web Scraping")

        # Inicjalizacja zawartości zakładek
        self.init_connect_db_tab()
        self.init_basic_tests_tab()
        self.init_statistics_tab()
        self.init_charts_tab()
        self.init_custom_query_tab()
        self.init_web_scraping_tab()
        #inicjalizacja klienta mongo
        self.client = None

    #################### CONNECT ####################
    def init_connect_db_tab(self): # DONE 
        layout = QVBoxLayout()
        self.connect_db_tab.setLayout(layout)

        # Przycisk do połączenia z bazami danych
        button_connect_postgres = QPushButton("PostgresSQL")
        button_connect_postgres.clicked.connect(self.postgresql_connect)
        layout.addWidget(button_connect_postgres)

        # Rozłączanie z bazy danych
        button_terminate_postgres = QPushButton("Terminate PostgresSQL")
        button_terminate_postgres.clicked.connect(self.postgresql_terminate)
        layout.addWidget(button_terminate_postgres)

        # Przycisk do połączenia z bazami danych
        button_connect_mongodb = QPushButton("MongoDB")
        button_connect_mongodb.clicked.connect(self.mongodb_connect)
        layout.addWidget(button_connect_mongodb)

        # Rozłączanie z bazy danych
        button_terminate_mongodb = QPushButton("Terminate MongoDB")
        button_terminate_mongodb.clicked.connect(self.mongodb_terminate)
        layout.addWidget(button_terminate_mongodb)

        self.db_status = QLabel()
        layout.addWidget(self.db_status)

    def postgresql_config(self, filename='database.ini', section='postgresql'): # DONE 
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(filename)

        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(
                'Section {0} not found in the {1} file'.format(section, filename))

        return db

    def postgresql_connect(self): # DONE 
        try:
            # read connection parameters
            params = self.postgresql_config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**params)

            # create a cursor
            self.cur = self.conn.cursor()
            self.chooseDatabase = 1

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        self.db_status.setText("Connected PostgreSQL")

    def postgresql_terminate(self): # DONE 
        self.cur.close()
        if self.conn is not None:
            self.conn.close()
            print('Database connection closed.')
            self.chooseDatabase = 0

        self.db_status.setText("Terminated PostgreSQL")

    def mongodb_connect(self): # DONE 
        try:
            # Connect to the MongoDB server
            print('Connecting to the MongoDB server...')
            self.client = MongoClient('mongodb://localhost:27017/')

            # Access the desired database
            self.db = self.client['librarytestmongo']

            self.chooseDatabase = 2
            
        except Exception as error:
            print(error)

        self.db_status.setText("Connected MongoDB")

    def mongodb_terminate(self): # DONE
        # Close the MongoDB connection if it exists
        if self.client is not None:
            self.client.close()
            self.chooseDatabase = 0
            print('MongoDB connection closed.')
        elif self.client is None:
            print('You dont have a working connection!')

        self.db_status.setText("Terminated MongoDB")

    #################### BASIC TESTS ####################
    def init_basic_tests_tab(self):  # DONE 
        layout = QVBoxLayout()
        self.basic_tests_tab.setLayout(layout)

        self.test_text = QTextEdit()
        layout.addWidget(self.test_text)

        # Przyciski do testów
        insert_button = QPushButton("INSERT")
        insert_button.clicked.connect(lambda: self.execute_tests(1))
        layout.addWidget(insert_button)

        modify_button = QPushButton("MODIFY")
        modify_button.clicked.connect(lambda: self.execute_tests(2))
        layout.addWidget(modify_button)

        delete_button = QPushButton("DELETE")
        delete_button.clicked.connect(lambda: self.execute_tests(3))
        layout.addWidget(delete_button)

        self.basic_tests_label = QLabel()
        layout.addWidget(self.basic_tests_label)

    def execute_tests(self, i):  # DONE 
        numbers = self.test_text.toPlainText()
        if self.chooseDatabase == 1:
            match i:
                case 1:
                    self.execute_insert(int(numbers))
                case 2:
                    self.execute_modify(int(numbers))
                case 3:
                    self.execute_delete(int(numbers))
        elif self.chooseDatabase == 2:
            match i:
                case 1:
                    self.execute_mongo_insert(int(numbers))
                case 2:
                    self.execute_mongo_modify(int(numbers))
                case 3:
                    self.execute_mongo_delete(int(numbers))
        else:
            self.basic_tests_label.setText("Connect to database first")

    def execute_insert(self, tries=1000):  # DONE 
        # Tutaj dodaj kod odpowiedzialny za wykonanie polecenia INSERT
        self.cur.execute("SELECT COUNT(*) FROM types")
        self.conn.commit()
        max_types = self.cur.fetchone()

        start_time = time.time()
        for i in range(tries):
            self.cur.execute("""INSERT INTO types VALUES (%s, %s)""", (int(max_types[0] + i + 1), 'test'))
            self.conn.commit()

        execution_time = time.time() - start_time
        self.basic_tests_label.setText("Execution time: " + str(execution_time))

    def execute_modify(self, tries=1000):  # DONE 
        self.cur.execute("SELECT COUNT(*) FROM types")
        self.conn.commit()
        max_types = self.cur.fetchone()

        start_time = time.time()
        for i in range(tries):
            self.cur.execute("""UPDATE types SET name = %s WHERE typeId = %s""", ('newtest', int(max_types[0] + -i)))
            self.conn.commit()

        execution_time = time.time() - start_time
        print("Czas wykonania: " + str(execution_time))
        self.basic_tests_label.setText("Execution time: " + str(execution_time))

    def execute_delete(self, tries=1000):  # DONE
        start_time = time.time()
        for i in range(tries):
            self.cur.execute("""DELETE FROM types
                            WHERE typeId = (SELECT typeId
                            FROM types
                            ORDER BY typeId DESC
                            LIMIT 1)
                            """)
            self.conn.commit()

        execution_time = time.time() - start_time
        print("Czas wykonania: " + str(execution_time))
        self.basic_tests_label.setText("Execution time: " + str(execution_time))

    def execute_mongo_insert(self, tries=1000): # DONE
        mycol = self.db["types"]
        max_types = self.db.types.count_documents({})
        start_time = time.time()
        for i in range(tries):
            mycol.insert_one({"typeId": max_types + i + 1, "name":"test"})

        execution_time = time.time() - start_time
        self.basic_tests_label.setText("Execution time: " + str(execution_time))

    def execute_mongo_modify(self, tries=1000): # DONE
        mycol = self.db["types"]
        max_types = self.db.types.count_documents({})
        start_time = time.time()
        
        for i in range(tries):
            mycol.update_one({"typeId": max_types-i}, {"$set":{"name":"newtest"}})

        execution_time = time.time() - start_time
        self.basic_tests_label.setText("Execution time: " + str(execution_time))
    
    def execute_mongo_delete(self, tries=1000): # DONE
        mycol = self.db["types"]
        max_types = self.db.types.count_documents({})
        start_time = time.time()

        for i in range(tries):
            mycol.delete_one({"typeId": max_types-i})

        execution_time = time.time() - start_time
        self.basic_tests_label.setText("Execution time: " + str(execution_time))

    #################### STATISTICS ####################
    def init_statistics_tab(self):  # DONE
        layout = QVBoxLayout()
        self.statistics_tab.setLayout(layout)

        # Przykładowe statystyki
        stats_label = QLabel("Statystyki:")
        layout.addWidget(stats_label)

        self.listwidget = QListWidget()
        self.listwidget.insertItem(0, "books")
        self.listwidget.insertItem(1, "authors")
        self.listwidget.insertItem(2, "types")
        self.listwidget.insertItem(3, "students")
        self.listwidget.insertItem(4, "max_pagecount")
        self.listwidget.insertItem(5, "min_pagecount")
        self.listwidget.clicked.connect(self.stat_clicked)
        layout.addWidget(self.listwidget)

        self.stat_label = QLabel()
        layout.addWidget(self.stat_label)

    def stat_clicked(self):  # DONE
        item = self.listwidget.currentItem()
        if self.chooseDatabase == 1:
            match item.text():
                case "books":
                    self.stat_books()
                case "authors":
                    self.stat_authors()
                case "types":
                    self.stat_types()
                case "students":
                    self.stat_students()
                case "max_pagecount":
                    self.stat_max_pagecount()
                case "min_pagecount":
                    self.stat_min_pagecount()
        elif self.chooseDatabase == 2:
            match item.text():
                case "books":
                    self.stat_mongo_books()
                case "authors":
                    self.stat_mongo_authors()
                case "types":
                    self.stat_mongo_types()
                case "students":
                    self.stat_mongo_students()
                case "max_pagecount":
                    self.stat_mongo_max_pagecount()
                case "min_pagecount":
                    self.stat_mongo_min_pagecount()
        else:
            self.stat_label.setText("Connect to database first")
        
    def stat_books(self):  # DONE 
        query = """SELECT COUNT(*) FROM books"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            self.stat_label.setText(str(self.cur.fetchall()))
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

    def stat_authors(self):  # DONE 
        query = """SELECT COUNT(*) FROM authors"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            self.stat_label.setText(str(self.cur.fetchall()))
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

    def stat_types(self):  # DONE 
        query = """SELECT COUNT(*) FROM types"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            self.stat_label.setText(str(self.cur.fetchall()))
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

    def stat_students(self):  # DONE 
        query = """SELECT COUNT(*) FROM students"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            self.stat_label.setText(str(self.cur.fetchall()))
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

    def stat_max_pagecount(self):  # DONE 
        query = """SELECT MAX(pagecount) FROM books"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            self.stat_label.setText(str(self.cur.fetchall()))
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

    def stat_min_pagecount(self):  # DONE 
        query = """SELECT MIN(pagecount) FROM books"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            self.stat_label.setText(str(self.cur.fetchall()))
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

    def stat_mongo_books(self): # DONE
        self.stat_label.setText(str(self.db.books.count_documents({})))
    
    def stat_mongo_authors(self): # DONE
        self.stat_label.setText(str(self.db.authors.count_documents({})))

    def stat_mongo_types(self): # DONE
        self.stat_label.setText(str(self.db.types.count_documents({})))

    def stat_mongo_students(self): # DONE
        self.stat_label.setText(str(self.db.students.count_documents({})))

    def stat_mongo_max_pagecount(self): # DONE
        result = self.db.books.find().sort("pagecount", -1).limit(1)
        for document in result:
            print(document)
        self.stat_label.setText(str(document["pagecount"]))

    def stat_mongo_min_pagecount(self): # DONE
        result = self.db.books.find().sort("pagecount", 1).limit(1)
        for document in result:
            print(document)
        self.stat_label.setText(str(document["pagecount"]))\
        
    #################### CHARTS ####################
    def init_charts_tab(self):  # DONE
        layout = QVBoxLayout()
        self.charts_tab.setLayout(layout)

        charts_label = QLabel("Wykresy:")
        layout.addWidget(charts_label)

        self.chartslist = QListWidget()
        self.chartslist.insertItem(0, "Borrows")
        self.chartslist.insertItem(1, "Generes")
        self.chartslist.clicked.connect(self.choose_graph)
        layout.addWidget(self.chartslist)

        self.charts_status_label = QLabel()
        layout.addWidget(self.charts_status_label)

    def choose_graph(self):  # 
        item = self.chartslist.currentItem()
        if self.chooseDatabase == 1:
            match item.text():
                case "Borrows":
                    self.borrows_graph()
                case "Generes":
                    self.generes_graph()
        # elif self.chooseDatabase == 2:
        #     match item.text():
        #         case "Borrows":
        #             self.borrows_mongo_graph()
        #         case "Generes":
        #             self.generes__mongo_graph()
        else:
            self.charts_status_label.setText("Connect to database first")

    def borrows_graph(self):  # DONE
        self.borrowsGraph = pg.PlotWidget()
        self.setCentralWidget(self.borrowsGraph)

        query = """SELECT studentId, COUNT(*) AS total_borrows FROM borrows GROUP BY studentId ORDER BY total_borrows DESC"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            res = self.cur.fetchall()
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

        students = []
        books = []
        for i in res:
            students.append(i[0])
            books.append(i[1])

        self.borrowsGraph.setBackground('w')

        pen = pg.mkPen(color=(255, 0, 0))
        self.borrowsGraph.plot(students, books, pen=pen)

    def generes_graph(self):  # DONE
        self.generesGraph = pg.PlotWidget()
        self.setCentralWidget(self.generesGraph)

        query = """SELECT COUNT(*) as number, name FROM types GROUP BY name ORDER BY number DESC"""
        try:
            self.cur.execute(query)
            self.conn.commit()
            res = self.cur.fetchall()
        except Exception as e:
            self.stat_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")

        iterator = []
        generes = []
        count = []
        a = 1
        for i in res:
            iterator.append(a)
            count.append(i[0])
            generes.append((a, i[1]))

            a = a + 1

        self.generesGraph.setBackground('w')

        pen = pg.mkPen(color=(255, 0, 0))
        self.generesGraph.plot(iterator, count, pen=pen, labels=generes)
        ax = self.generesGraph.getAxis('bottom')
        ax.setTicks([generes])

    #################### CUSTOM QUERY ####################
    def init_custom_query_tab(self):  # DONE 
        layout = QVBoxLayout()
        self.custom_query_tab.setLayout(layout)

        self.query_text = QTextEdit()
        self.query_text.setPlaceholderText("POSTGRES: standard SQL query.\nMONGODB: 'nameOfTheCollection'|query ")
        layout.addWidget(self.query_text)

        self.execute_button = QPushButton("Wykonaj zapytanie")
        self.execute_button.clicked.connect(self.execute_custom_query)
        layout.addWidget(self.execute_button)

        self.result_label = QLabel()
        layout.addWidget(self.result_label)

    def execute_custom_query(self):  # DONE 
        # Tutaj dodaj kod odpowiedzialny za wykonanie polecenia SQL podanego przez użytkownika
        query = self.query_text.toPlainText()

        if self.chooseDatabase == 1:
            try:
                self.cur.execute(query)
                self.conn.commit()
                self.result_label.setText(str(self.cur.fetchall()))
            except Exception as e:
                self.result_label.setText(f"Błąd podczas wykonywania zapytania:\n{str(e)}")
        elif self.chooseDatabase == 2:
            l = query.split("|")
            print(l)
            all = []
            print(str(l[0]))
            mycol = self.db[str(l[0])]
            print(str(l[1]))
            mydoc = mycol.find(dict(eval(l[1])))
            for x in mydoc:
                all.append(x)
            self.result_label.setText(str(all))
        else:
            self.result_label.setText("Connect to database first")

    #################### WEB SCRAPING ####################
    def init_web_scraping_tab(self):  # DONE
        layout = QVBoxLayout()
        self.web_scraping_tab.setLayout(layout)

        self.scraping_text = QTextEdit()
        layout.addWidget(self.scraping_text)

        self.scraping_button = QPushButton("Wykonaj zapytanie")
        self.scraping_button.clicked.connect(self.web_scraping)
        layout.addWidget(self.scraping_button)

        self.scraping_res_label = QLabel()
        layout.addWidget(self.scraping_res_label)

    def web_scraping(self):  # DONE
        url = self.scraping_text.toPlainText()

        page = urlopen(url)

        html_bytes = page.read()
        time.sleep(5)


        html = html_bytes.decode("utf-8")

        book = []

        book_name_search = re.search("\"Book\"\,\"name\"\s*:\s*\"([^\"]+)\"", html)
        book_name = self.match_regex(book_name_search)
        book.append(book_name)

        number_of_pages_search = re.search("\"*\",\"numberOfPages\"\s*:\s*([^\,]+)", html)
        number_of_pages = self.match_regex(number_of_pages_search)
        if number_of_pages == None:
            number_of_pages = 0
        book.append(number_of_pages)

        point_score_search = re.search("\"AggregateRating\"\,\"ratingValue\"\s*:\s*([^\,]+)", html)
        point_score = self.match_regex(point_score_search)
        if point_score == None:
            point_score = 0
        book.append(point_score)

        author_name_search = re.search("\"Person\"\,\"name\"\s*:\s*\"([^\"]+)\"", html)
        author_name = self.match_regex(author_name_search)
        book.append(author_name)

        genre_search = re.search(
            "<a\s+href=\"https:\/\/www\.goodreads\.com\/genres\/(\w+)\"\s+class=\"Button\s+Button--tag-inline\s+Button--small\">",
            html)
        genre = self.match_regex(genre_search)
        book.append(genre)

        print(book)

        if self.chooseDatabase == 1:
            self.scraping_res_label.setText(str(book))

            self.cur.execute("SELECT COUNT(*) FROM types")
            self.conn.commit()
            max_types = self.cur.fetchone()

            self.cur.execute("SELECT COUNT(*) FROM authors")
            self.conn.commit()
            max_authors = self.cur.fetchone()

            self.cur.execute("SELECT COUNT(*) FROM books")
            self.conn.commit()
            max_books = self.cur.fetchone()

            self.cur.execute("""INSERT INTO types VALUES (%s, %s)""", (int(max_types[0] + 1), genre))
            self.conn.commit()
            self.cur.execute("""INSERT INTO authors VALUES (%s, %s)""", (int(max_authors[0] + 1), author_name))
            self.conn.commit()
            self.cur.execute("""INSERT INTO books VALUES (%s, %s, %s, %s, %s, %s)""", (
            int(max_books[0] + 1), book_name, number_of_pages, round(float(point_score)), int(max_authors[0] + 1),
            int(max_types[0] + 1)))
            self.conn.commit()
            self.scraping_res_label.setText(str(book))

        elif self.chooseDatabase == 2:
            mycol = self.db["books"]
            max_types_books = self.db.books.count_documents({}) +1
            max_types_authors = self.db.authors.count_documents({}) +1
            max_types_types = self.db.types.count_documents({}) +1
            mycol.insert_one({"id": max_types_books, "name":book_name, "pagecount":number_of_pages, "point": round(float(point_score)), "authorId": max_types_authors, "typeId": max_types_types})

            mycol = self.db["authors"]
            mycol.insert_one({"authorId": max_types_authors, "name": author_name})

            mycol = self.db["types"]
            mycol.insert_one({"typeId": max_types_types, "name": genre})

            self.scraping_res_label.setText(str(book))

        else:
            self.scraping_res_label.setText("Connect to database first")

    def match_regex(self, match):  # DONE 
        if match:
            rgx = match.group(1)
            return rgx


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
