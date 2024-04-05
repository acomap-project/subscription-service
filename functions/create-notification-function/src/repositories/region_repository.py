import json
import os

root_dir = os.path.join(
    os.path.dirname(os.path.join(os.path.abspath(__file__))), 
    '../..'
)

def list_areas_of_city(city_code: str):
    try:
        with open(os.path.join(root_dir, 'static/region_data.json'), encoding='utf-8') as file:
            data = json.load(file)

            area_list = [area for area in data if area['city_code'] == city_code]
            return area_list
    except Exception as e:
        print(e)
        raise Exception(f'Cannot open file: static/region_data.json')
    
def get_area(city_code: str, area_code: str):
    try:
        with open(os.path.join(root_dir, 'static/region_data.json'), encoding='utf-8') as file:
            data = json.load(file)
            area = [area for area in data if area['city_code'] == city_code and area['area_code'] == area_code]
            if area is not None and len(area) > 0:
                return area[0]
            return None
    except Exception as e:
        print(e)
        raise Exception(f'Cannot open file: static/region_data.json')
    
def list_all_cities():
    try:
        with open(os.path.join(root_dir, 'static/region_data.json'), encoding='utf-8') as file:
            data = json.load(file)

            city_list = list(set([(area['city_code'], area['city_name']) for area in data]))
            return city_list
    except Exception as e:
        print(e)
        raise Exception(f'Cannot open file: static/region_data.json')