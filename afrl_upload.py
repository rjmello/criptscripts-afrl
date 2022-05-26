from getpass import getpass
import math

import pandas as pd
import cript


def get_citation(row, citations):
    # Check if citation was already created
    if row["reference"] in citations.keys():
        print(f"Found existing reference: {reference.title}")
        return citations[row["reference"]]

    # Create reference
    reference = cript.Reference(group=group, title=row["reference"], public = True)
    if "doi.org" in row["reference"]:
        reference.doi = row["reference"].replace("doi.org", "").strip("/")

    # Save reference
    try:
        api.save(reference)
        print(f"Created reference: {reference.title}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch reference from the DB if it already exists
        reference = api.get(cript.Reference, {"title": reference.title, "created_by": api.user.uid})
        print(f"Found existing reference: {reference.title}")

    # Create citation
    citation = cript.Citation(reference=reference)
    citations[citation.reference.title] = citation

    return citation



def get_polymer(row, polymers, citation):
    name = row["polymer"]
    cas = row["polymer_CAS"]
    bigsmiles = _convert_to_bigsmiles(row["polymer_SMILES"])
    mw_w = row["polymer_Mw"]
    mw_d = row["polymer_PDI"]

    # Return repeats
    unique_set = (mw_w, mw_d, citation)
    if unique_set in polymers.keys():
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
        "name": name,
        "identifiers": identifiers,
        "properties": properties,
        "public": True
    }
    polymer = cript.Material(**polymer_dict)

    # Save material
    try:
        api.save(polymer)
        print(f"Created polymer: {polymer.name}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch and update existing material
        polymer = api.get(cript.Material, {"name": polymer.name, "created_by": api.user.uid})
        _setattrs(polymer, **polymer_dict)
        api.save(polymer)
        print(f"Updated existing polymer: {polymer.name}")

    polymers[unique_set] = polymer
    return polymer


def get_solvent(row):
    solvent = api.get(
        cript.Material, 
        {
            "identifiers": [
                {
                    "key": "cas", 
                    "value": row["solvent_CAS"]
                }
            ], 
            "group": cript_group.uid
        }
    )
    print(f"Found existing solvent: {solvent.name}")
    return solvent


def get_mixture(row, polymer, solvent, citation):
    name = f"{polymer.name} + {solvent.name} mixture"
    conc_vol_fraction = row["polymer_vol_frac"]
    conc_mass_fraction = row["polymer_wt_frac"]
    temp_cloud = row["cloud_point_temp"]
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

    # Create new material object
    mixture_dict = {
        "group": group,
        "name": name,
        "identifiers": identifiers,
        "components": components,
        "properties": properties,
        "public": True
    }
    mixture = cript.Material(**mixture_dict)

    # Save material
    try:
        api.save(mixture)
        print(f"Created mixture: {mixture.name}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch and update existing material
        mixture = api.get(cript.Material, {"name": mixture.name, "created_by": api.user.uid})
        _setattrs(mixture, **mixture_dict)
        api.save(mixture)
        print(f"Updated existing mixture: {mixture.name}")

    return mixture


def get_inventory():
    inventory = cript.Inventory(group=group, collection=collection, name="linear_polymer_3pdb", materials=[])

    # Save inventory
    try:
        api.save(inventory)
        print(f"Created Inventory: {inventory.name}")
    except cript.exceptions.DuplicateNodeError:
        # Fetch inventory from the DB if it already exists
        inventory = api.get(cript.Inventory, {"name": inventory.name, "group": group.uid})
        print(f"Found existing inventory: {inventory.name}")

    return inventory


def _convert_to_bigsmiles(old_smiles):
    # Replace * with [<] and [>]
    bigsmiles = "[<]".join(old_smiles.split("*", 1))
    bigsmiles = "[>]".join(bigsmiles.rsplit("*", 1))
    
    return f"{{[]{bigsmiles}[]}}"


def _setattrs(obj, **kwargs):
    for key, value in kwargs.items():
        setattr(obj, key, value)


if __name__ == "__main__":
    host = input("Host (e.g., criptapp.org): ")
    token = getpass("API Token: ")
    group_name = input("Group name: ")
    collection_name = input("Collection name: ")
    path = input("Path to CSV file: ")
    citations = {}
    polymers = {}
    inventory_list = []

    # Establish connection with the API
    api = cript.API(host, token, tls=False)

    # Fetch Group objects
    group = api.get(cript.Group, {"name": group_name})
    cript_group = api.get(cript.Group, {"name": "CRIPT"})

    # Fetch Collection object
    collection = api.get(cript.Collection, {"name": collection_name, "group": group.uid})

    # Upload data
    df = pd.read_csv(path)
    for index, row in df.iterrows():
        print(f"\nRow {index}")
        print("*************************")

        citation = get_citation(row, citations)  # Reuse for each object in row
        polymer = get_polymer(row, polymers, citation)
        solvent = get_solvent(row)
        mixture = get_mixture(row, polymer, solvent, citation)
        inventory_list += [polymer, solvent, mixture]

        print("*************************\n")

    # Add materials to inventory
    inventory = get_inventory()
    inventory.materials += inventory_list
    api.save(inventory)

