# -*- coding: utf-8 -*-
import logging

logger = logging.getLogger('intake-esm')
handle = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s ' '- %(message)s')
handle.setFormatter(formatter)
logger.addHandler(handle)
