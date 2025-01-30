import ctypes
from enum import Enum
import numpy as np
import ray
from ctypes.util import find_library
from ctypes import POINTER, c_void_p, c_uint, c_long

# Load Intel MKL shared library
mkl_lib = ctypes.cdll.LoadLibrary(find_library('mkl_rt'))

# Structures
class MKL_Complex8(ctypes.Structure): 
    __fields__ = [("real", ctypes.c_float), ("imag", ctypes.c_float)]


# Define the datatype for the handle and constants
class DFTI_DESCRIPTOR(ctypes.Structure):
    _fields_ = []

DFTI_DESCRIPTOR_HANDLE = POINTER(DFTI_DESCRIPTOR)

DFTI_CONFIG_VALUE = c_uint
DFTI_SINGLE = 35  # Single precision floating point
DFTI_COMPLEX = 32  # Complex number data type

# Define the bindings similarly to the Rust bindings
DftiCreateDescriptor = mkl_lib.DftiCreateDescriptor
DftiCreateDescriptor.restype = c_long
DftiCreateDescriptor.argtypes = [POINTER(DFTI_DESCRIPTOR_HANDLE), DFTI_CONFIG_VALUE, DFTI_CONFIG_VALUE, c_long]

DftiFreeDescriptor = mkl_lib.DftiFreeDescriptor
DftiFreeDescriptor.restype = c_long
DftiFreeDescriptor.argtypes = [POINTER(DFTI_DESCRIPTOR_HANDLE)]


DftiCommitDescriptor = mkl_lib.DftiCommitDescriptor
DftiCommitDescriptor.restype = c_long
DftiCommitDescriptor.argtypes = [DFTI_DESCRIPTOR_HANDLE]

DftiComputeForward = mkl_lib.DftiComputeForward
DftiComputeForward.restype = c_long
DftiComputeForward.argtypes = [DFTI_DESCRIPTOR_HANDLE, c_void_p]


class MKLFFT:
    def __init__(self, size):
        self.size = size
        self.descriptor = DFTI_DESCRIPTOR_HANDLE()

        # Create DFTI descriptor for single precision complex 1D FFT
        status = DftiCreateDescriptor(ctypes.byref(self.descriptor), DFTI_SINGLE, DFTI_COMPLEX, 1, self.size)
        if status != 0:
            raise ValueError("MKL DftiCreateDescriptor failed with status: " + str(status))

        # Commit the descriptor
        status = DftiCommitDescriptor(self.descriptor)
        if status != 0:
            raise ValueError("MKL DftiCommitDescriptor failed with status: " + str(status))

    def compute_forward(self, data):
        # Perform in-place FFT
        data_ptr = data.ctypes.data_as(ctypes.c_void_p)
        status = DftiComputeForward(self.descriptor, data_ptr)
        if status != 0:
            raise ValueError("MKL DftiComputeForward failed with status: " + str(status))

    def __del__(self):
        # Free the descriptor
        status = DftiFreeDescriptor(ctypes.byref(self.descriptor))
        if status != 0:
            print("Warning: MKL DftiFreeDescriptor failed with status:", status)

def generate_random_complex_float_array(n, dtype=np.complex64):
    array = np.random.rand(n) + 1j*np.random.rand(n)
    return array.astype(dtype)

def generate_set_complex_float_array(n, dtype=np.complex64):
    # generate array with elements from 1+1j up to n+nj
    array = np.array([x+1j*x for x in range(1,n+1)])
    return array.astype(dtype)

@ray.remote
def vector_to_matrix(vector):
    n = int(np.sqrt(len(vector)))

    # case where len(vector) is a perfect square
    if n*n == len(vector):
        return vector.reshape((n, n))
    else:
       print(f'Length used: {len(vector)}')
       raise ValueError('Length of vector is not a perfect square')

@ray.remote
class FFTActor:
  def __init__(self, size):
      self.mkl_fft = MKLFFT(size)

  def compute_fft(self, data):
      self.mkl_fft.compute_forward(data)
      return True 