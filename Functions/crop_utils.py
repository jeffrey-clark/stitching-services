
import numpy as np
import warnings
from shapely.geometry import Polygon
from rasterio.features import rasterize
import rasterio



def load_img(file):
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=rasterio.errors.NotGeoreferencedWarning)
        with rasterio.open(file) as raw_ds:
            if raw_ds.count > 1:
                img = raw_ds.read().mean(0)  # Average across bands if multiple bands
                img = img.astype(np.uint8)  # Convert from float64 to uint8
            else:
                img = raw_ds.read(1)  # Read the first band

        return img
    

def calculate_box(img, threshold=20, starting_from='side', mode='levels'):
    std_x = np.std(img, axis=0)
    std_y = np.std(img, axis=1)
    
    if mode=='differences':
        diff_x = np.diff(std_x)
        
        left = diff_x.copy()
        left[int(len(left)/2):] = 0
        xmin = np.argmax(left)
        
        right = diff_x.copy()
        right[:int(len(right)/2)] = 0
        xmax = np.argmin(right)
        
        diff_y = np.diff(std_y)
        
        top = diff_y.copy()
        top[int(len(top)/2):] = 0
        ymin = np.argmax(top)
        
        bottom = diff_y.copy()
        bottom[:int(len(bottom)/2)] = 0
        ymax = np.argmin(bottom)
    
    else:
        if starting_from == 'side':
            above_threshold_x = np.argwhere(std_x > threshold)
            above_threshold_y = np.argwhere(std_y > threshold)
            if len(above_threshold_x)>0:
                xmin = above_threshold_x[0][0]
                xmax = above_threshold_x[-1][0]
            else:
                xmin = 0
                xmax = img.shape[1]

            if len(above_threshold_y)>0:
                ymin = above_threshold_y[0][0]
                ymax = above_threshold_y[-1][0]
            else:
                ymin = 0
                ymax = img.shape[0]


        elif starting_from == 'center':
            idx_x = np.arange(len(std_x))
            idx_y = np.arange(len(std_y))

            left_x = np.argwhere((std_x < threshold) & (idx_x < len(std_x)/2))
            right_x = np.argwhere((std_x < threshold) & (idx_x > len(std_x)/2))

            above_y = np.argwhere((std_y < threshold) & (idx_y < len(std_y)/2))
            below_y = np.argwhere((std_y < threshold) & (idx_y > len(std_y)/2))

            xmin = left_x[-1][0] if len(left_x)>0 else 0
            xmax = right_x[0][0] if len(right_x)>0 else img.shape[1]


            ymin = above_y[-1][0] if len(above_y)>0 else 0
            ymax = below_y[0][0] if len(below_y)>0 else img.shape[0]
        else:
            raise NotImplementedError()
    
    # prevent cropping mask from being too small: it must be at least half the original picture
    # Here we say that if the cropping is more than half the image we consider the cropping as invalid
    # and we reset to the default of no cropping on that side

    if xmin > img.shape[1]/2:
        xmin = 0
    if xmax < img.shape[1]/2:
        xmax = img.shape[1]
    if ymin > img.shape[0]/2:
        ymin = 0
    if ymax < img.shape[0]/2:
        ymax = img.shape[0]
    
    return xmin, xmax, ymin, ymax


# def get_variance_polygon(img_path, threshold=20, sigma=10):

#     img = load_img(img_path)

#     if sigma is not None:
#         img = gaussian_filter(img, sigma=10)
#     xmin, xmax, ymin, ymax = calculate_box(img, threshold)

#     return Polygon([[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax]])


# def get_variance_bounds(img_path, threshold=20, sigma=10):
    
#     img = load_img(img_path)
    
#     if sigma is not None:
#         img = gaussian_filter(img, sigma=10)
#     xmin, xmax, ymin, ymax = calculate_box(img, threshold)

#     return xmin, xmax, ymin, ymax   


def geometry_from_segmentation(segm):
    assert len(segm)%2 == 0
    n_pairs = len(segm)//2
    polygon = []
    for pair in range(n_pairs):
        polygon.append([segm[2*pair], segm[2*pair + 1]])
    return Polygon(polygon)


def cornered_frame(xmin, xmax, ymin, ymax, corners_width, corners_height, number_padding_x):
    height = ymax - ymin
    width = xmax - xmin
    return Polygon([
            [xmin, ymin + corners_height*height],
            [xmin + corners_width*width, ymin + corners_height*height],
            [xmin + corners_width*width, ymin],
            [xmax - corners_width*width, ymin],
            [xmax - corners_width*width, ymin + corners_height*height],
            [xmax, ymin + corners_height*height],
            [xmax, ymax - corners_height*height],
            [xmax - corners_width*width, ymax - corners_height*height],
            [xmax - corners_width*width, ymax],
            [xmin + (corners_width + number_padding_x)*width, ymax],
            [xmin + (corners_width + number_padding_x)*width, ymax - corners_height*height],
            [xmin, ymax - corners_height*height]])


def midpoint_frame(xmin, xmax, ymin, ymax, clip_long_side, clip_short_side, number_padding_x, number_padding_y):
    height = ymax - ymin
    width = xmax - xmin    
    return Polygon([
        [xmin, ymin + number_padding_y*height],
        [xmin + number_padding_x*width, ymin + number_padding_y*height],
        [xmin + number_padding_x*width, ymin],
        [xmin + (1 - clip_long_side)*width/2, ymin],
        [xmin + (1 - clip_long_side)*width/2, ymin + clip_short_side*height],
        [xmin + (1 + clip_long_side)*width/2, ymin + clip_short_side*height],
        [xmin + (1 + clip_long_side)*width/2, ymin],
        [xmax, ymin],
        [xmax, ymin + (1 - clip_long_side)*height/2],
        [xmax - clip_short_side*width, ymin + (1  - clip_long_side) * height/2],
        [xmax - clip_short_side*width, ymin + (1 + clip_long_side) * height/2],
        [xmax, ymin + (1 + clip_long_side) * height/2],
        [xmax, ymax],
        [xmin + (1 + clip_long_side)*width/2, ymax],
        [xmin + (1 + clip_long_side)*width/2, ymax - clip_short_side*height],
        [xmin + (1 - clip_long_side)*width/2, ymax - clip_short_side*height],
        [xmin + (1 - clip_long_side)*width/2, ymax],
        [xmin + number_padding_x * width, ymax],
        [xmin + number_padding_x * width, ymax - number_padding_y * height],
        [xmin, ymax - number_padding_y * height]]
    )


def v_frame(xmin, xmax, ymin, ymax, v_height, v_width, number_padding_x, number_padding_y, 
            corners_width, corners_height):
    height = ymax - ymin
    width = xmax - xmin    
    return Polygon([
        [xmin, ymin],
        [xmin + (1 - v_width)*width/2, ymin],
        [xmin + (1 - v_width)*width/2, ymin + v_height*height],
        [xmin + (1 + v_width)*width/2, ymin + v_height*height],
        [xmin + (1 + v_width)*width/2, ymin],
        [xmax - corners_width*width, ymin],
        [xmax - corners_width*width, ymin + corners_height*height],
        [xmax, ymin + corners_height*height],
        [xmax, ymax],
        [xmin + (1 + v_width)*width/2, ymax],
        [xmin + (1 + v_width)*width/2, ymax - v_height*height],
        [xmin + (1 - v_width)*width/2, ymax - v_height*height],
        [xmin + (1 - v_width)*width/2, ymax],
        [xmin + number_padding_x * width, ymax],
        [xmin + number_padding_x * width, ymax - number_padding_y * height],
        [xmin, ymax - number_padding_y * height]]
    )
    

def square_frame(xmin, xmax, ymin, ymax, number_padding_x, number_padding_y, upper_left, bottom_right):
    height = ymax - ymin
    width = xmax - xmin
    if upper_left != 0:
        poly = Polygon([
            [xmin, ymin + number_padding_y*height],
            [xmin + number_padding_x*width, ymin + number_padding_y*height],
            [xmin + number_padding_x*width, ymin],
            [xmax, ymin],
            [xmax, ymax],
            [xmin, ymax]])
    elif bottom_right!= 0:
        poly = Polygon([
            [xmin, ymin],
            [xmax, ymin],
            [xmax, ymax - number_padding_y*height],
            [xmax - number_padding_x*width, ymax - number_padding_y*height],
            [xmax - number_padding_x*width, ymax],
            [xmin, ymax]])
    # Default: number on bottom left
    else:
        poly = Polygon([
            [xmin, ymin],
            [xmax, ymin],
            [xmax, ymax],
            [xmin + number_padding_x*width, ymax],
            [xmin + number_padding_x*width, ymax - number_padding_y*height],
            [xmin, ymax - number_padding_y*height]])            
    return poly


def get_framed_polygon(mask_bounds, params):
    
    xmin, xmax, ymin, ymax = mask_bounds
    
    # margins are expressed in hundreds of pixels
    # so that their numerical values remain closer to unity as possible
    xmin = xmin + (params['margin_left']) * 100
    xmax = xmax - (params['margin_right']) * 100
    ymin = ymin + (params['margin_top']) * 100
    ymax = ymax - (params['margin_bottom']) * 100
    
    if params['corners'] == 1:
        poly = cornered_frame(xmin, xmax, ymin, ymax, params['corners_width'], 
                              params['corners_height'], params['number_padding_x'])
    elif params['midpoints'] == 1:
        poly = midpoint_frame(xmin, xmax, ymin, ymax, params['clip_long_side'], 
                              params['clip_short_side'], params['number_padding_x'], params['number_padding_y'])
    elif params['v_shape'] == 1:
        poly = v_frame(xmin, xmax, ymin, ymax, params['v_height'], 
                       params['v_width'], params['number_padding_x'], params['number_padding_y'],
                           params['corners_width'], params['corners_height'])
    else:
        poly = square_frame(xmin, xmax, ymin, ymax, params['number_padding_x'], 
                            params['number_padding_y'], params['number_ul'], params['number_r'])      
    return poly



def generate_cropping_mask(img_shape, mask_bounds, cropping_parameters):

    # data = np.array([mask_bounds[0], mask_bounds[1], mask_bounds[2], mask_bounds[3], 
    #      cropping_parameters['corners'], 
    #      cropping_parameters['midpoints'], 
    #      cropping_parameters['t_lg'], 
    #      cropping_parameters['b_lg'], 
    #      cropping_parameters['l_lg'], 
    #      cropping_parameters['r_lg'], 
    #      cropping_parameters['number_ul'], 
    #      cropping_parameters['number_r']])
    
    pred_polygon = get_framed_polygon(mask_bounds, cropping_parameters)
    
    mask = rasterize([pred_polygon], out_shape=img_shape)
    return mask
