import concurrent.futures
from fuzzywuzzy import fuzz
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

path_excel_root = "C:\\Users\Admin\OneDrive\Desktop\\file_product_dienmay\\file_can_merge\\3-cellphones.xlsx"
path_excel_top = "C:\\Users\Admin\OneDrive\Desktop\\file_product_dienmay\\file_can_merge\\file_tong_hop - cellphone.xlsx"


def match_products(row_dmx, data_dict_tgdd):
    lst_partial_ratio = []
    for row_tgdd in data_dict_tgdd:
        if fuzz.partial_ratio(row_dmx.get('name_custom'), row_tgdd.get('name_custom')) > 85:
            result = f"{row_tgdd.get('name_custom')}: {row_tgdd.get('product_base_url')}"
            print(result)
            lst_partial_ratio.append(result)
    return lst_partial_ratio


# Load your data into pandas DataFrames
df_dmx = pd.read_excel(path_excel_root)
df_tgdd = pd.read_excel(path_excel_top)

# Convert DataFrames to lists of dictionaries
data_dict_dmx = df_dmx.to_dict(orient='records')
data_dict_tgdd = df_tgdd.to_dict(orient='records')
print("DMX")

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []
    for row_dmx in data_dict_dmx:
        futures.append(executor.submit(match_products, row_dmx=row_dmx, data_dict_tgdd=data_dict_tgdd))

    # for future in concurrent.futures.as_completed(futures):
    #     print("Completed => ", future.result())

# Use ThreadPoolExecutor to parallelize the matching process
with ThreadPoolExecutor(max_workers=400) as executor:
    futures = [executor.submit(match_products, cell_dmx, data_dict_tgdd) for cell_dmx in data_dict_dmx]

    for future in concurrent.futures.as_completed(futures):
        try:
            print("Completed!!!!")
            print(future)
        except Exception as e:
            print("Has Error")
    # Collect the results
    lst_output_dict = []
    for cell_dmx, future in zip(data_dict_dmx, concurrent.futures.as_completed(futures)):
        lst_partial_ratio = future.result()
        dict_partial_ratio = {"value_similar": lst_partial_ratio}
        cell_dmx.update(dict_partial_ratio)
        lst_output_dict.append(cell_dmx)

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(lst_output_dict)
    df['value_similar'] = df['value_similar'].apply(lambda x: "\n".join(x) if isinstance(x, list) else x)

    # Save the DataFrame to an Excel file
    output_excel_path = "xlsx_files/cellphones_match_85.xlsx"
    df.to_excel(output_excel_path, index=False)
