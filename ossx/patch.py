from oss2 import models, utils

from .utils import make_crc_adapter

setattr(models, "make_crc_adapter", make_crc_adapter)
setattr(utils, "make_crc_adapter", make_crc_adapter)
setattr(models, "make_progress_adapter", utils.make_progress_adapter)
setattr(utils, "make_progress_adapter", utils.make_progress_adapter)
