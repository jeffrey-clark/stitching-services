import os
import Functions.utilities as u

# read the config file here
cfg = u.read_config()

def generate_config_file(df, machine_name):

    contract_name = df.contract_name.values[0]
    # add verification here that there is only one contract name in the data


    if machine_name.lower() == "savio":
        paths = cfg['savio']
        image_folders = [os.path.join(paths['images_folder'], x) + "/" for x in df.folder] 


    config_data = {
        "alg_kwargs": {
            "hessian_threshold": 100
        },
        "algorithm": "SURF",
        "pool_workers": 30,
        "surf_workers": 15,
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
        "folders": image_folders,
        "hessian_threshold": 100,
        "img_cache_folder": os.path.join(paths['cache_folder'], contract_name, "SURF"),
        "checkpoint_cache_folder": os.path.join(paths['cache_folder'], contract_name),
        "lowe_ratio": 0.7,
        "min_inliers": 10,
        "min_matches": 10,
        "min_swath_size": 1,
        "ransac_reproj_threshold": 10.0,
        "raster_output_folder": os.path.join(paths['results_folder'], contract_name),
        "swath_break_threshold": 30,
        "swath_folder": os.path.join(paths['cache_folder'], contract_name),
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

    pass
    