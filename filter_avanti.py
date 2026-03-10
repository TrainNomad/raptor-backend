import os
import csv

# Configuration
SOURCE_DIR = "./gtfs/UK_Rail"
TARGET_DIR = "./gtfs/Avanti_Only"
TARGET_AGENCY_ID = "VT"

if not os.path.exists(TARGET_DIR):
    os.makedirs(TARGET_DIR)

def filter_gtfs():
    print(f"--- Début de l'extraction pour l'agence : {TARGET_AGENCY_ID} ---")

    # 1. Filtrer AGENCY.TXT
    print("Filtrage de agency.txt...")
    with open(f"{SOURCE_DIR}/agency.txt", 'r', encoding='utf-8') as f_in, \
         open(f"{TARGET_DIR}/agency.txt", 'w', encoding='utf-8', newline='') as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row['agency_id'] == TARGET_AGENCY_ID:
                writer.writerow(row)

    # 2. Filtrer ROUTES.TXT et mémoriser les route_ids
    print("Filtrage de routes.txt...")
    route_ids = set()
    with open(f"{SOURCE_DIR}/routes.txt", 'r', encoding='utf-8') as f_in, \
         open(f"{TARGET_DIR}/routes.txt", 'w', encoding='utf-8', newline='') as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row['agency_id'] == TARGET_AGENCY_ID:
                writer.writerow(row)
                route_ids.add(row['route_id'])

    # 3. Filtrer TRIPS.TXT et mémoriser les trip_ids et service_ids
    print("Filtrage de trips.txt...")
    trip_ids = set()
    service_ids = set()
    with open(f"{SOURCE_DIR}/trips.txt", 'r', encoding='utf-8') as f_in, \
         open(f"{TARGET_DIR}/trips.txt", 'w', encoding='utf-8', newline='') as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row['route_id'] in route_ids:
                writer.writerow(row)
                trip_ids.add(row['trip_id'])
                service_ids.add(row['service_id'])

    # 4. Filtrer STOP_TIMES.TXT (Le plus lourd, traité ligne par ligne)
    print("Filtrage de stop_times.txt (cela peut prendre du temps)...")
    stop_ids = set()
    with open(f"{SOURCE_DIR}/stop_times.txt", 'r', encoding='utf-8') as f_in, \
         open(f"{TARGET_DIR}/stop_times.txt", 'w', encoding='utf-8', newline='') as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            if row['trip_id'] in trip_ids:
                writer.writerow(row)
                stop_ids.add(row['stop_id'])

    # 5. Filtrer STOPS.TXT
    print("Filtrage de stops.txt...")
    with open(f"{SOURCE_DIR}/stops.txt", 'r', encoding='utf-8') as f_in, \
         open(f"{TARGET_DIR}/stops.txt", 'w', encoding='utf-8', newline='') as f_out:
        reader = csv.DictReader(f_in)
        writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            # On garde l'arrêt s'il est utilisé par Avanti ou si c'est une station parente
            if row['stop_id'] in stop_ids or row['location_type'] == '1':
                writer.writerow(row)

    # 6. Filtrer CALENDAR.TXT
    print("Filtrage de calendar.txt...")
    if os.path.exists(f"{SOURCE_DIR}/calendar.txt"):
        with open(f"{SOURCE_DIR}/calendar.txt", 'r', encoding='utf-8') as f_in, \
             open(f"{TARGET_DIR}/calendar.txt", 'w', encoding='utf-8', newline='') as f_out:
            reader = csv.DictReader(f_in)
            writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
            writer.writeheader()
            for row in reader:
                if row['service_id'] in service_ids:
                    writer.writerow(row)

    # 7. Copier FEED_INFO.TXT (Optionnel)
    if os.path.exists(f"{SOURCE_DIR}/feed_info.txt"):
        import shutil
        shutil.copyfile(f"{SOURCE_DIR}/feed_info.txt", f"{TARGET_DIR}/feed_info.txt")

    print(f"--- Terminé ! Les données sont dans {TARGET_DIR} ---")

if __name__ == "__main__":
    filter_gtfs()