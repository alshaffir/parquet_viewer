import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget, QPushButton, QFileDialog
from pyarrow.parquet import ParquetFile

app = QApplication(sys.argv)  # Create the QApplication instance
windows = []  # Create an empty list to store references to the windows
open_windows = []  # Global list to keep track of open windows

def create_table(data):
    table = QTableWidget()
    table.setColumnCount(len(data[0]))
    table.setRowCount(len(data))
    for row, row_data in enumerate(data):
        for col, col_data in enumerate(row_data):
            item = QTableWidgetItem(str(col_data))
            table.setItem(row, col, item)
    return table

def load_parquet_file(file_path):
    try:
        # Load the Parquet file
        parquet_file = ParquetFile(file_path)
        # Retrieve the column names
        column_names = parquet_file.schema.names
        # Retrieve the row data
        row_data = parquet_file.read().to_pandas().values.tolist()
        # Create the table with row data only, do not include column names
        table = create_table(row_data)  
        # Return the table and column names
        return table, column_names
    except Exception as e:
        print(f"Error loading Parquet file: {str(e)}")
        return None, []


def create_window():
    window = QMainWindow()
    window.setWindowTitle("Parquet Viewer")
    window.setGeometry(100, 100, 400, 300)  # Adjust window size

    central_widget = QWidget()
    global layout  # Access the global layout variable
    layout = QVBoxLayout()

    # Enhance UI of Load Parquet file button
    load_button = QPushButton("Load Parquet File")
    load_button.clicked.connect(load_parquet_button_clicked)
    load_button.setStyleSheet(
        """
        QPushButton {
            background-color: #4CAF50;
            color: white;
            padding: 15px 32px;
            text-align: center;
            text-decoration: none;
            font-size: 16px;
            margin: 4px 2px;
            border-radius: 12px;
        }
        QPushButton:hover {
            background-color: #45a049;
        }
        """
    )
    layout.addWidget(load_button)

    # Add "Close all" button
    close_button = QPushButton("Close all")
    close_button.clicked.connect(app.quit)  # Close application on click
    close_button.setStyleSheet(
        """
        QPushButton {
            background-color: #f44336;
            color: white;
            padding: 15px 32px;
            text-align: center;
            text-decoration: none;
            font-size: 16px;
            margin: 4px 2px;
            border-radius: 12px;
        }
        QPushButton:hover {
            background-color: #da190b;
        }
        """
    )
    layout.addWidget(close_button)

    # Table to display Parquet data
    table = QTableWidget()
    layout.addWidget(table)

    # Set the layout on the central widget
    central_widget.setLayout(layout)
    window.setCentralWidget(central_widget)

    # Show the window
    window.show()
    sys.exit(app.exec_())

def load_parquet_button_clicked():
    # Open file dialog to select Parquet file
    file_path, _ = QFileDialog.getOpenFileName(None, "Select Parquet File")
    if file_path:
        # Load Parquet file and display the data
        table, column_names = load_parquet_file(file_path)
        if table:
            # Create a new window
            table_window = QMainWindow()
            table_window.setWindowTitle("Parquet Table")
            # Set the new window's location to be offset by the number of open windows
            table_window.setGeometry(100 + (20 * len(open_windows)), 100 + (20 * len(open_windows)), 800, 600)
            table_window.setCentralWidget(table)
            # Set column names
            table.setHorizontalHeaderLabels(column_names)
            # Show the new window
            table_window.show()
            # Add the new window to the list of open windows
            open_windows.append(table_window)


def create_table_window(table):
    # Create a new window
    table_window = QMainWindow()
    table_window.setWindowTitle("Parquet Table")
    table_window.setGeometry(100, 100, 800, 600)
    table_window.setCentralWidget(table)
    # Append the window to the windows list
    windows.append(table_window)
    # Show the new window
    table_window.show()

# Create the main window
create_window()
