from distutils.core import setup

import py2exe  # noqa: F401


setup(
    name="camera_capture",
    console=[{"script": "camera_capture.py"}],
    options={
        "py2exe": {
            "bundle_files": 1,
            "compressed": True,
            "optimize": 2,
            "includes": ["cv2", "numpy"],
        }
    },
    zipfile=None,
)
