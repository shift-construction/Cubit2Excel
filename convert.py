import xml.etree.ElementTree as ET
import pandas as pd
import zipfile
import os
import glob

def parse_trade_nodes(node, level=0, path=""):
    data = []
    for trade_node in node.findall("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}TradeNode"):
        code_elem = trade_node.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}Code")
        billref_elem = trade_node.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}BillReference")
        description_elem = trade_node.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}Description")

        code = code_elem.text if code_elem is not None else ""
        billref = billref_elem.text if billref_elem is not None else ""
        description = description_elem.text if description_elem is not None else ""

        new_path = f"{path}/{code}" if path else code

        # Add trade node data
        trade_node_data = {
            'Level': level,
            'Path': new_path,
            'Code': code,
            'Description': description,
            'Bill Reference': billref
        }

        # Process EstimatingComponents
        estimating_components = trade_node.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}EstimatingComponents")
        if estimating_components is not None:
            for component in estimating_components.findall("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}EstimatingComponent"):
                component_data = trade_node_data.copy()  # Start with trade node data
                for element in component:
                    tag = element.tag.split('}')[1]
                    component_data[tag] = element.text
                data.append(component_data)
        else:
            # If no estimating components, add the trade node data
            data.append(trade_node_data)

        # Recursively process child trade nodes
        child_trade_nodes = trade_node.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}TradeNodes")
        if child_trade_nodes is not None:
            data.extend(parse_trade_nodes(child_trade_nodes, level + 1, new_path))

        # Process CompositeRateSheet if no child trade nodes
        else:
            parent_quantity = trade_node_data.get('Quantity')
            parent_unit = trade_node_data.get('Unit')
            composite_rate_sheet = trade_node.find(".//{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}CompositeRateSheet")
            if composite_rate_sheet is not None:
                rate_items = parse_rate_items(composite_rate_sheet, new_path, 99, parent_quantity, parent_unit)
                data.extend(rate_items)

    return data

def parse_rate_items(composite_rate_sheet, path, level, parent_quantity, parent_unit):
    rate_items = []
    for rate_item in composite_rate_sheet.findall("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}RateItem"):
        item_data = {
            'Level': level,
            'Path': path,
            'Description': rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}Description").text,
            'Rate': rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}Rate").text,
            'Quantity': rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}Quantity").text,
            'Unit': rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}Unit").text,
            'Total': rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}Total").text,
            'Parent Quantity': parent_quantity,
            'Parent Unit': parent_unit,
            'WastageFactor': rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}WastageFactor").text,
            'Factor': rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}Factor").text
        }
        rate_codes = rate_item.find("{http://www.buildsoft.com.au/xmlschemas/2012/05/BT2}RateCodes")
        if rate_codes is not None:

            job_sort_code_data = rate_codes.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}JobSortCodeData")

            if job_sort_code_data is not None:
                assigned_code = job_sort_code_data.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}AssignedCode")
                print(assigned_code.text)
                if assigned_code is not None:
                    item_data['Code'] = assigned_code.text
        rate_items.append(item_data)
    return rate_items



def unpivot_data(data):
    unpivoted_data = []
    for record in data:
        if record.get('Bill Reference') or record.get('Assigned Code'):  # Include records with a Bill Reference or Assigned Code
            path_parts = record['Path'].split('/')
            unpivoted_record = {
                'Bill Reference': record.get('Bill Reference', ''),
                'Assigned Code': record.get('Assigned Code', '')
            }
            for level, part in enumerate(path_parts):
                level_key = f'Level {level + 1}'
                description = next((rec['Description'] for rec in data if rec.get('Code') == part), "")
                unpivoted_record[level_key] = description
            
            # Include additional columns
            additional_columns = {k: v for k, v in record.items() if k not in ['Path', 'Code', 'Description', 'Bill Reference', 'Assigned Code', 'Level']}
            unpivoted_record.update(additional_columns)
            
            unpivoted_data.append(unpivoted_record)
    return unpivoted_data
def process_cbx_file(cbx_file_path, all_data, all_unpivoted_data):
    # Extract XML file from the zip archive
    with zipfile.ZipFile(cbx_file_path, 'r') as zip_ref:
        zip_ref.extract('TakeoffJob.xml')
    
    # Load extracted XML file
    tree = ET.parse('TakeoffJob.xml')
    root = tree.getroot()
    
    # Parse XML and collect data
    root_trade_container = root.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}RootTradeContainer")
    trade_nodes = root_trade_container.find("{http://schemas.datacontract.org/2004/07/Buildsoft.PhoenixTakeOff.DataEntities.DataTransferObjects}TradeNodes")
    data = parse_trade_nodes(trade_nodes)
    
    # Add CBX file name to each record
    cbx_file_name = os.path.basename(cbx_file_path)
    for record in data:
        record['CBX File'] = cbx_file_name
    
    # Append data to all_data
    all_data.extend(data)
    
    # Create unpivoted DataFrame
    unpivoted_data = unpivot_data(data)
    for record in unpivoted_data:
        record['CBX File'] = cbx_file_name
    
    # Append unpivoted data to all_unpivoted_data
    all_unpivoted_data.extend(unpivoted_data)
    
    # Clean up the extracted file
    os.remove('TakeoffJob.xml')

def main(folder_path):
    all_data = []
    all_unpivoted_data = []
    
    # Process each CBX file in the folder
    for cbx_file_path in glob.glob(os.path.join(folder_path, '*.CBX')):
        print(f"Processing {cbx_file_path}")
        process_cbx_file(cbx_file_path, all_data, all_unpivoted_data)
    
    # Create DataFrame
    df = pd.DataFrame(all_data)
    
    # Create unpivoted DataFrame
    df_unpivoted = pd.DataFrame(all_unpivoted_data)
    
    # Ensure the columns are ordered correctly
    max_levels = max(len(record['Path'].split('/')) for record in all_data)
    columns = ['CBX File', 'Bill Reference', 'Assigned Code'] + [f'Level {i}' for i in range(1, max_levels + 1)] + list(set(df_unpivoted.columns) - set(['CBX File', 'Bill Reference', 'Assigned Code'] + [f'Level {i}' for i in range(1, max_levels + 1)]))
    df_unpivoted = df_unpivoted.reindex(columns=columns)
    
    # Export to Excel with multiple sheets
    output_file = 'output_materials_correct.xlsx'
    with pd.ExcelWriter(output_file) as writer:
        df.to_excel(writer, sheet_name='Original Data', index=False)
        df_unpivoted.to_excel(writer, sheet_name='Unpivoted Data', index=False)

if __name__ == "__main__":
    folder_path = ''  # Change this to the actual path of your folder
    main(folder_path)
