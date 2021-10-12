import sys
import json
from assignment import Assignment


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: Nike assignment data files <file> <calendar_data_file> <product_data_file> <sales_data_file> "
              "<store_data_file> <output_data_file>", file=sys.stderr)
        sys.exit(-1)

    calendar_data_file = sys.argv[1]
    product_data_file = sys.argv[2]
    sales_data_file = sys.argv[3]
    store_data_file = sys.argv[4]
    output_data_file = sys.argv[5]

    data_assignment = Assignment(calendar_data_file, product_data_file, sales_data_file, store_data_file, output_data_file)
    data_assignment.process_data()



