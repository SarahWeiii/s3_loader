import numpy as np
import s3_loader.s3_io as s3_io
import cv2

s3 = s3_io.s3_init()

a = s3_io.load_s3_json(s3, "meshlrm-objaverse/views_6b522020f8354b30a45d91fbfd290873/opencv_cameras.json")
print(a.keys())
img = s3_io.load_s3_image(s3, "meshlrm-objaverse/views_000e246a674e4d36904c6101923ccb03/color_00_in_00.png")
print(np.array(img).shape)
normal = s3_io.load_s3_exr(s3, "meshlrm-objaverse/views_000e246a674e4d36904c6101923ccb03/normal_00_in_00.exr")
print(normal.shape)
txt = s3_io.load_s3_txt(s3, "xiwei-test/dataset_paths/s3_objaverse_two_obj_debug.txt")
print(txt[:5])
exist = s3_io.file_exists_in_s3(s3, "meshlrm-objaverse/views_000e246a674e4d36904c6101923ccb03/normal_0s0_in_00.exr")
print(exist)
list = s3_io.list_files_in_folder(s3, "meshlrm-objaverse/views_000e246a674e4d36904c6101923ccb03")
print(len(list))

rook_s3 = s3_io.s3_init(s3_url="http://rook-ceph-rgw-haosu.rook-haosu")
images = s3_io.load_s3_image_batch(rook_s3, list[:8])
images[0].show()

normal_list = [f for f in list if f.endswith(".exr") and 'normal' in f]
normals = s3_io.load_s3_exr_batch(rook_s3, normal_list[:8])
cv2.imwrite('normal.png', normals[0]*255)
cv2.imwrite('normal.exr', normals[0])

depth_list = [f for f in list if f.endswith(".exr") and 'depth' in f]
depths = s3_io.load_s3_exr_batch(rook_s3, depth_list[:8])
cv2.imwrite('depth.png', depths[0][...,0]*255)


list = s3_io.list_files_in_folder(s3, "shapedex-info")
print(len(list))
print(list[:5])

s3_io.download_file_from_s3(s3, "meshlrm-objaverse/views_000e246a674e4d36904c6101923ccb03/color_00_in_00.png", "a.png")
s3_io.upload_file_to_s3(s3, "a.png", "xiwei-test/color_00_in_00.png")
s3_io.delete_file_in_s3(s3, "xiwei-test/color_00_in_00.png")

s3_io.delete_folder_in_s3(s3, "xiwei-test/wildimages")