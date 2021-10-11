import sys
from assignment import Assignment


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: Nike assignment data files <file> <calendar_data_file> <product_data_file> <sales_data_file> "
              "<store_data_file> ", file=sys.stderr)
        sys.exit(-1)

    calendar_data_file = sys.argv[1]
    product_data_file = sys.argv[2]
    sales_data_file = sys.argv[3]
    store_data_file = sys.argv[4]

    data_assignment = Assignment(calendar_data_file, product_data_file, sales_data_file, store_data_file)
    dataframes = data_assignment.process_data()


