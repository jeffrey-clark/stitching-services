import os
import Functions.utilities as u
import yaml
import json

# read the config file here
cfg = u.read_config()

def serialize_for_google_sheet(data):
    """
    Serializes complex data types (like lists and dictionaries) to JSON strings.
    Handles None, True, and False explicitly.
    """
    if isinstance(data, (dict, list)):
        return json.dumps(data)
    elif data is None:
        return 'null'  # Serialize None as 'null'
    elif isinstance(data, bool):
        return str(data).lower()  # Serialize boolean as 'true' or 'false'
    return data

def deserialize_from_google_sheet(data):
    """
    Deserializes data from a JSON string back to Python objects.
    Handles 'null', 'true', and 'false' explicitly.
    """
    if data == 'null':
        return None  # Deserialize 'null' as None
    elif data == 'true':
        return True  # Deserialize 'true' as True
    elif data == 'false':
        return False  # Deserialize 'false' as False

    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return data

def generate_default_config_data(df, machine):

    if 'path' in df:
        folders_list = df.path.values.tolist()  # Convert to a standard Python list if it's a pandas Series
        folders = json.dumps(folders_list)
    else:
        folders = json.dumps([])  # Default to an empty list


    config_data = {
        "alg_kwargs": {"hessian_threshold": 100},
        "algorithm": "SURF",
        "cropping_parameters": {
            "b_lg": 1,
            "clip_long_side": 0.04818053378598765,
            "clip_short_side": 0.01,
            "corners": 1,
            "corners_height": 0.06207328840129208,
            "corners_width": 0.08519120208022454,
            "l_lg": 1,
            "margin_bottom": 8,
            "margin_left": 8,
            "margin_lg": 2,
            "margin_right": 8,
            "margin_top": 8,
            "midpoints": 0,
            "number_padding_x": 0.1,
            "number_padding_y": 0.07,
            "number_r": 0,
            "number_ul": 0,
            "r_lg": 1,
            "t_lg": 1,
            "v_shape": 0
        },
        "cropping_std_threshold": 20,
        "cropping_filter_sigma": None,
        "cropping_origin": "side",
        "cropping_mode": "levels",
        "crs": "EPSG:3857",
        "folders": folders,
        "hessian_threshold": 100,
        "lowe_ratio": 0.7,
        "min_inliers": 10,
        "min_matches": 10,
        "min_swath_size": 1,
        "ransac_reproj_threshold": 10.0,
        "swath_break_threshold": 30,
        "swath_reproj_threshold": 50,
        "threads_per_worker": None,
        "subsample_swath": True,
        "early_stopping": False,
        "across_swath_threshold": 100,  # Used in case of early stopping
        "response_threshold": 400,
        "cluster_inlier_threshold": 50,
        "cluster_link_method": "max",
        "individual_link_threshold": 10,  # If link is below this threshold, it does not contribute to the total inliers between swaths
        "artifact_angle_threshold": 2,
        "soft_break_threshold": None,
        "soft_individual_threshold": 30,
        "optim_inclusion_threshold": 10,
        "inlier_threshold": 50,
        "suspect_artifacts": True,
        "strict_inlier_threshold": 200,
        "optim_inlier_threshold": 50,
        "n_within": 2,
        "n_across": 1,
        "n_swath_neighbors": 2,
        "retry_threshold": 10,
        "n_iter": 100000,
        "optim_lr_theta": 0.05,
        "optim_lr_scale": 0.05,
        "optim_lr_xy": 0.05,
        "raster_edge_size": 5000,
        "raster_edge_constraint_type": "max"
    }

    if machine.lower() == "savio":
        config_data['pool_workers'] = 40
        config_data['surf_workers'] = 8
    elif machine.lower() == "google_vm":
        config_data['pool_workers'] = 30
        config_data['surf_workers'] = 6

    return config_data


def config_format(config_data):
    # Define the desired order of keys and their formatting
    key_order = [
        ("alg_kwargs", True),  # (key, is_complex_structure)
        ("algorithm", False),
        ("", False),  # Empty string for extra newline
        ("pool_workers", False),
        ("surf_workers", False),
        ("", False),
        ("cropping_parameters", True),
        ("cropping_std_threshold", False),
        ("cropping_filter_sigma", False),
        ("cropping_origin", False),
        ("cropping_mode", False),
        ("", False),
        ("crs", False),
        ("folders", True),
        ("", False),
        ("hessian_threshold", False),
        ("img_cache_folder", False),
        ("checkpoint_cache_folder", False),
        ("lowe_ratio", False),
        ("min_inliers", False),
        ("min_matches", False),
        ("min_swath_size", False),
        ("ransac_reproj_threshold", False),
        ("raster_output_folder", False),
        ("", False),
        ("swath_break_threshold", False),
        ("swath_folder", False),
        ("swath_reproj_threshold", False),
        ("threads_per_worker", False),
        ("", False),
        ("subsample_swath", False),
        ("early_stopping", False),
        ("across_swath_threshold", False),
        ("response_threshold", False),
        ("", False),
        ("cluster_inlier_threshold", False),
        ("cluster_link_method", False),
        ("individual_link_threshold", False),
        ("artifact_angle_threshold", False),
        ("soft_break_threshold", False),
        ("soft_individual_threshold", False),
        ("", False),
        ("optim_inclusion_threshold", False),
        ("inlier_threshold", False),
        ("suspect_artifacts", False),
        ("strict_inlier_threshold", False),
        ("optim_inlier_threshold", False),
        ("n_within", False),
        ("n_across", False),
        ("n_swath_neighbors", False),
        ("retry_threshold", False),
        ("n_iter", False),
        ("optim_lr_theta", False),
        ("optim_lr_scale", False),
        ("optim_lr_xy", False),
        ("", False),
        ("raster_edge_size", False),
        ("raster_edge_constraint_type", False),
        ("", False),
        ("collection_regex", True),
    ]

    exclude_if_None = ['collection_regex']

    yaml_lines = []
    for key, is_complex in key_order:
        if key:
            value = config_data.get(key)
            if value == None and key in exclude_if_None:
                continue
            if is_complex:
                # Complex structures (lists, dicts)
                dumped_value = yaml.dump({key: value}, default_flow_style=False, sort_keys=False).split('\n')
                yaml_lines.extend(dumped_value[:-1])
            else:
                # Formatting simple values
                if isinstance(value, bool):
                    formatted_value = str(value).lower()
                elif value is None:
                    formatted_value = 'null'
                else:
                    formatted_value = value
                line = f"{key}: {formatted_value}"
                yaml_lines.append(line)
        else:
            # Add an extra newline for spacing
            yaml_lines.append("")

    formatted_yaml = '\n'.join(yaml_lines)
    return formatted_yaml



def export_config_file(contract_status, country, config_data, machine_name):

    status = contract_status.data
    contract_alias = status['contract_name']
    symlink_folders = contract_status.load_symlinks(machine_name)

    paths = cfg[machine_name.lower()]

    # Machine-specific processing
    if machine_name.lower() == "savio":
        # Assuming 'folders' is already a list of paths
        if symlink_folders is not None:
            image_folders = [x + "/" for x in symlink_folders]
        else:
            image_folders = [os.path.join(paths['images_folder'], country, os.path.basename(os.path.dirname(x))) + "/" for x in config_data['folders']]
    
        # Machine-specific paths
        machine_specific_data = {
            "img_cache_folder": os.path.join(paths['cache_folder'], contract_alias, "SURF"),
            "checkpoint_cache_folder": os.path.join(paths['cache_folder'], contract_alias),
            "raster_output_folder": os.path.join(paths['results_folder'], contract_alias),
            "swath_folder": os.path.join(paths['cache_folder'], contract_alias),
        }

    elif machine_name.lower() == "google_vm":
        dp = cfg['google_vm']['docker_paths']

        image_folders = [os.path.join(dp['images_folder'].rstrip('/'), country, os.path.basename(os.path.dirname(x))) + "/" for x in config_data['folders']]
    
        # Machine-specific paths
        machine_specific_data = {
            "img_cache_folder": os.path.join(dp['cache_folder'], contract_alias, "SURF"),
            "checkpoint_cache_folder": os.path.join(dp['cache_folder'], contract_alias),
            "raster_output_folder": os.path.join(dp['results_folder'], contract_alias),
            "swath_folder": os.path.join(dp['cache_folder'], contract_alias)
        }

    elif machine_name.lower() == "tabei":
        image_folders = config_data['folders']
        
        # Machine-specific paths
        machine_specific_data = {
            "img_cache_folder": os.path.join(paths['cache_folder'], contract_alias, "SURF"),
            "checkpoint_cache_folder": os.path.join(paths['cache_folder'], contract_alias),
            "raster_output_folder": os.path.join(paths['results_folder'], contract_alias),
            "swath_folder": os.path.join(paths['cache_folder'], contract_alias),
        }

    
    else:  # For 'tabei' and other machines
        #image_folders = config_data['folders']  # Already in the correct format
        raise ValueError("NO CONFIG DATA FOR PROVIDED MACHINE")
    

    # Update config_data with the processed image_folders
    config_data['folders'] = image_folders

    # Combine common and machine-specific configuration data
    config_data.update(machine_specific_data)

     # Serialize config_data using the custom format function
    custom_yaml_content = config_format(config_data)

    # Write the custom YAML content to file
    os.makedirs(os.path.join("Files/config_files", machine_name.lower()), exist_ok=True)
    output_file_path = os.path.join("Files/config_files", machine_name.lower(),  f"{contract_alias}.yml")
    with open(output_file_path, 'w') as file:
        file.write(custom_yaml_content)

    print(f"Configuration file exported: {output_file_path}")
    return output_file_path