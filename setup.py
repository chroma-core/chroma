from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext
import subprocess
import os
import sys


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        # Make sure the output directory in a lib directory in the package
        cmake_library_output_dir = os.path.join(
            extdir, self.distribution.get_name(), "lib"
        )

        cmake_args = [
            f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={cmake_library_output_dir}",
            f"-DPYTHON_EXECUTABLE={sys.executable}",
        ]

        cfg = "Debug" if self.debug else "Release"
        build_args = ["--config", cfg]

        cmake_args += [f"-DCMAKE_BUILD_TYPE={cfg}"]
        build_args += ["--", "-j8"]

        env = os.environ.copy()
        env[
            "CXXFLAGS"
        ] = f'{env.get("CXXFLAGS", "")} -DVERSION_INFO=\\"{self.distribution.get_version()}\\"'

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        print(f"CMAKE_LIBRARY_OUTPUT_DIRECTORY: {cmake_library_output_dir}")
        print(f"Build temp directory: {self.build_temp}")

        subprocess.check_call(
            ["cmake", ext.sourcedir] + cmake_args, cwd=self.build_temp, env=env
        )
        subprocess.check_call(
            ["cmake", "--build", "."] + build_args, cwd=self.build_temp
        )


setup(
    name="chromadb",
    packages=find_packages(),
    ext_modules=[CMakeExtension("chromadb")],
    cmdclass={"build_ext": CMakeBuild},
    package_data={
        "chromadb": ["*.so", "*.dylib", "*.dll", "*.pyd"],
    },
    include_package_data=True,
)
