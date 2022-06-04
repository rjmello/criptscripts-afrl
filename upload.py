from getpass import getpass
import yaml
import math

import pandas as pd
import cript


def get_citation(index, row):
    reference_title = row["reference"]

    # Check if citation was already created
    if row["reference"] in citations.keys():
        print(f"ROW {index + 2} -- Found existing reference: {reference_title}")
        return citations[row["reference"]]

    # Create reference
    reference = cript.Reference(group=group, title=reference_title, doi="", public=True)
    if "doi.org" in reference_title:
        reference.doi = reference_title.replace("doi.org", "").strip("/")

    # Save reference
    try:
        api.save(reference, max_level=0)
        print(f"ROW {index + 2} -- Created reference: {reference.title}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch reference from the DB if it already exists
        reference = api.get(cript.Reference, {"title": reference.title, "created_by": api.user.uid}, max_level=0)
        print(f"ROW {index + 2} -- Found existing reference: {reference.title}")

    # Create citation
    citation = cript.Citation(reference=reference)
    citations[citation.reference.title] = citation

    return citation


def get_inventory(inventory_name):
    inventory = cript.Inventory(group=group, collection=collection, name=inventory_name, materials=[], public=True)

    # Save inventory
    try:
        api.save(inventory, max_level=0)
        print(f"Created Inventory: {inventory.name}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch inventory from the DB if it already exists
        inventory = api.get(cript.Inventory, {"name": inventory.name, "group": group.uid}, max_level=0)
        print(f"Found existing inventory: {inventory.name}")

    return inventory


def get_solvent(index, row):
    cas = row["solvent_CAS"].strip()

    # Skip repeats
    if cas in solvents.keys():
        solvent = solvents[cas]
        print(f"ROW {index + 2} -- Found existing solvent: {solvent.name}")
        return solvent

    try:
        solvent = api.get(
            cript.Material, 
            {
                "identifiers": [
                    {
                        "key": "cas", 
                        "value": cas
                    }
                ], 
                "group": cript_group.uid
            },
            max_level=0
        )
        print(f"ROW {index + 2} -- Found existing solvent: {solvent.name}")
        solvents[cas] = solvent
        return solvent
    except cript.exceptions.APIGetError:
        return None


def get_polymer(index, row, citation):
    polymer_id = row["polymer_id"]
    name = row["polymer"]
    unique_name = name + f"_{polymer_id}"
    cas = row["polymer_CAS"]
    bigsmiles = _convert_to_bigsmiles(row["polymer_SMILES"])
    mw_w = row["polymer_Mw"]
    mw_d = row["polymer_PDI"]

    # Return repeats
    unique_set = (mw_w, mw_d, citation.reference.title)
    if unique_set in polymers.keys():
        print(f"ROW {index + 2} -- Found existing polymer: " + polymers[unique_set].name)
        return polymers[unique_set]

    # Create identifiers
    identifiers = []
    if name:
        identifiers.append(cript.Identifier(key="preferred_name", value=name))
    if cas:
        identifiers.append(cript.Identifier(key="cas", value=cas))
    if bigsmiles:
        identifiers.append(cript.Identifier(key="bigsmiles", value=bigsmiles))

    # Create properties
    properties = []
    if not math.isnan(mw_w):
        properties.append(cript.Property(key="mw_w", value=mw_w, unit="g/mol", citations=[citation]))
    if not math.isnan(mw_d):
        properties.append(cript.Property(key="mw_d", value=mw_d, citations=[citation]))

    # Create new material object
    polymer_dict = {
        "group": group,
        "name": unique_name,
        "identifiers": identifiers,
        "properties": properties,
        "public": True
    }
    polymer = cript.Material(**polymer_dict)

    # Save material
    try:
        api.save(polymer, max_level=0)
        print(f"ROW {index + 2} -- Created polymer: {polymer.name}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch and update existing material
        polymer = api.get(cript.Material, {"name": polymer.name, "created_by": api.user.uid}, max_level=0)
        _setattrs(polymer, **polymer_dict)
        api.save(polymer, max_level=0)
        print(f"ROW {index + 2} -- Updated existing polymer: {polymer.name}")

    polymers[unique_set] = polymer
    return polymer


def get_mixture(index, row, polymer, solvent, citation):
    mixture_id = row["mixture_id"]
    name = f"{polymer.name} + {solvent.name} mixture"
    unique_name = name + f" ({mixture_id})"
    conc_vol_fraction = row["polymer_vol_frac"]
    conc_mass_fraction = row["polymer_wt_frac"]
    temp_cloud = row["cloud_point_temp"]
    one_phase_direction = row["one_phase_direction"]
    pressure = row["pressure_MPa"]

    # Create identifiers
    identifiers = []
    if name:
        identifiers.append(cript.Identifier(key="preferred_name", value=name))

    # Create components
    components = [
        cript.Component(component_uid=1, component=polymer),
        cript.Component(component_uid=2, component=solvent)
    ]

    # Create properties
    properties = []
    if not math.isnan(conc_vol_fraction):
        properties.append(
            cript.Property(key="conc_vol_fraction", value=conc_vol_fraction, component_id=1, citations=[citation])
        )
    if not math.isnan(conc_mass_fraction):
        properties.append(
            cript.Property(key="conc_mass_fraction", value=conc_mass_fraction, component_id=1, citations=[citation])
        )
    if not math.isnan(temp_cloud):
        properties.append(
            cript.Property(
                key="temp_cloud", 
                value=temp_cloud, 
                unit="degC", 
                conditions=[],
                citations=[citation]
            )
        )
        if pressure:
            properties[-1].conditions.append(cript.Condition(key="pressure", value=pressure, unit="MPa"))
        if one_phase_direction:
            properties[-1].conditions.append(cript.Condition(key="+one_phase_direction", value=one_phase_direction))

    # Create new material object
    mixture_dict = {
        "group": group,
        "name": unique_name,
        "identifiers": identifiers,
        "components": components,
        "properties": properties,
        "public": True
    }
    mixture = cript.Material(**mixture_dict)

    # Save material
    try:
        api.save(mixture, max_level=0)
        print(f"ROW {index + 2} -- Created mixture: {mixture.name}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch and update existing material
        mixture = api.get(cript.Material, {"name": mixture.name, "created_by": api.user.uid}, max_level=0)
        _setattrs(mixture, **mixture_dict)
        api.save(mixture, max_level=0)
        print(f"ROW {index + 2} -- Updated existing mixture: {mixture.name}")

    mixtures[mixture.name] = mixture
    return mixture


def _convert_to_bigsmiles(old_smiles):
    # Replace * with [<] and [>]
    bigsmiles = "[<]".join(old_smiles.split("*", 1))
    bigsmiles = "[>]".join(bigsmiles.rsplit("*", 1))
    
    return f"{{[]{bigsmiles}[]}}"


def _setattrs(obj, **kwargs):
    for key, value in kwargs.items():
        setattr(obj, key, value)


def record_error(message):
    error_file = open("./errors.txt", "a")
    error_file.write(message + "\n\n")
    error_file.close()
    print(message)


def upload(index, row):
    citation = get_citation(index, row)  # Reuse for each object in row

    solvent = get_solvent(index, row)
    if solvent is None:
        # Record error and skip row if solvent is not found
        solvent_name = row["solvent"]
        solvent_cas = row["solvent_CAS"]
        record_error(f"ROW {index + 2} -- Solvent not found: {solvent_name} ({solvent_cas})")
        return

    # Update solvent inventory
    inventory_solvents.materials.append(solvent)
    api.save(inventory_solvents, max_level=0)
    print(f"ROW {index + 2} -- Updated solvent inventory.")

    polymer = get_polymer(index, row, citation)

    # Update polymer inventory
    inventory_polymers.materials.append(polymer)
    api.save(inventory_polymers, max_level=0)
    print(f"ROW {index + 2} -- Updated polymer inventory.")

    mixture = get_mixture(index, row, polymer, solvent, citation)

    # Update mixture inventory
    inventory_mixtures.materials.append(mixture)
    api.save(inventory_mixtures, max_level=0)
    print(f"ROW {index + 2} -- Updated mixture inventory.")


def load_config():
    try:
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        config = {}

    if config.get("host") is None:
        config["host"] = input("Host (e.g., criptapp.org): ")
    if config.get("token") is None:
        config["token"] = getpass("API Token: ")
    if config.get("group") is None:
        config["group"] = input("Group name: ")
    if config.get("collection") is None:
        config["collection"] = input("Collection name: ")
    if config.get("inventory") is None:
        config["inventory"] = input("Inventory name: ")
    if config.get("path") is None:
        config["path"] = input("Path to CSV file: ").strip('"')

    return config
    

if __name__ == "__main__":
    config = load_config()
    citations = {}
    solvents = {}
    polymers = {}
    mixtures = {}

    # Establish connection with the API
    api = cript.API(config["host"], config["token"])

    # Fetch objects
    group = api.get(cript.Group, {"name": config["group"]}, max_level=0)
    cript_group = api.get(cript.Group, {"name": "CRIPT"}, max_level=0)
    collection = api.get(cript.Collection, {"name": config["collection"], "group": group.uid}, max_level=0)
    inventory_solvents = get_inventory(config["inventory"] + " (solvents)")
    inventory_polymers = get_inventory(config["inventory"] + " (polymers)")
    inventory_mixtures = get_inventory(config["inventory"] + " (mixtures)")

    # Upload data
    df = pd.read_csv(config["path"])
    for index, row in df.iterrows():
        upload(index, row)