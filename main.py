from __future__ import print_function

import os
import time
from threading import Timer
import urllib.request
from PIL import Image
import pickle
import uuid
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products
from util import s3
import redis

from bluelens_log import Logging


AWS_OBJ_IMAGE_BUCKET = 'bluelens-style-object'
AWS_MOBILE_IMAGE_BUCKET = 'bluelens-style-mainimage'

OBJECT_IMAGE_WIDTH = 300
OBJECT_IMAGE_HEITH = 300
MOBILE_FULL_WIDTH = 375
MOBILE_THUMBNAIL_WIDTH = 200

MAX_PROCESS_NUM = 500

HEALTH_CHECK_TIME = 300
TMP_MOBILE_IMG = 'tmp_mobile_full.jpg'
TMP_MOBILE_THUMB_IMG = 'tmp_mobile_thumb.jpg'

SPAWN_ID = os.environ['SPAWN_ID']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')

REDIS_PRODUCT_CLASSIFY_QUEUE = 'bl:product:classify:queue'
REDIS_OBJECT_INDEX_QUEUE = 'bl:object:index:queue'
# REDIS_PRODUCT_HASH = 'bl:product:hash'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-image-processor')
product_api = Products()
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

heart_bit = True

def process_image(p_data):
  log.info('process_image')
  product = pickle.loads(p_data)

  make_mobile_images(product)

def make_mobile_image(image_name, type, image_path):

  if type == 'thumb':
    basewidth = MOBILE_THUMBNAIL_WIDTH
  else:
    basewidth = MOBILE_FULL_WIDTH

  try:
    f = urllib.request.urlopen(image_path)
    im = Image.open(f).convert('RGB')
    wpercent = (basewidth / float(im.size[0]))
    hsize = int((float(im.size[1]) * float(wpercent)))
    im = im.resize((basewidth, hsize), Image.ANTIALIAS)
    im.save(image_name + '.jpg')
    file_url = save_mobile_image_to_storage(image_name, type)
    return file_url
  except Exception as e:
    log.error('url open error')

def make_mobile_images(product):
  full_image = make_mobile_image(str(product['_id']), 'full', product['main_image'])
  if full_image == None:
    return
  thumb_image = make_mobile_image(str(product['_id']), 'thumb', product['main_image'])
  if thumb_image == None:
    return

  sub_images = []
  for sub_img in product['sub_images']:
    sub_image = make_mobile_image(str(uuid.uuid4()), 'sub', sub_img)
    sub_images.append(sub_image)

  product['main_image_mobile_full'] = full_image
  product['main_image_mobile_thumb'] = thumb_image
  product['sub_images_mobile'] = sub_images
  product['is_processed']= True
  update_product_to_db(product)

  rconn.lpush(REDIS_PRODUCT_CLASSIFY_QUEUE, pickle.dumps(product))

def save_mobile_image_to_storage(name, path):
  log.debug('save_mobile_image_to_storage')
  file = name + '.jpg'
  key = os.path.join(RELEASE_MODE, 'mobile', path, name + '.jpg')
  is_public = True
  file_url = storage.upload_file_to_bucket(AWS_MOBILE_IMAGE_BUCKET, file, key, is_public=is_public)
  log.info(file_url)
  return file_url

def update_product_to_db(product):
  log.debug('update_product_to_db')
  try:
    api_response = product_api.update_product_by_id(str(product['_id']), product)
    log.debug(api_response)
  except Exception as e:
    log.error("Exception when calling ProductApi->update_product_by_id: %s\n" % e)

def check_health():
  global  heart_bit
  log.info('check_health: ' + str(heart_bit))
  if heart_bit == True:
    heart_bit = False
    Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  else:
    delete_pod()

def delete_pod():
  log.info('exit: ' + SPAWN_ID)

  data = {}
  data['namespace'] = RELEASE_MODE
  data['id'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.setServerPassword(REDIS_PASSWORD)
  spawn.delete(data)

def dispatch_job(rconn):
  log.info('Start dispatch_job')
  Timer(HEALTH_CHECK_TIME, check_health, ()).start()

  count = 0
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_IMAGE_PROCESS_QUEUE])
    start_time = time.time()
    process_image(value)
    count = count + 1

    elapsed_time = time.time() - start_time
    log.info('image-processing time: ' + str(elapsed_time))

    if count > MAX_PROCESS_NUM:
      delete_pod()

    global  heart_bit
    heart_bit = True

if __name__ == '__main__':
  log.info('Start bl-image-processor:7')
  try:
    dispatch_job(rconn)
  except Exception as e:
    log.error(str(e))
    delete_pod()


  # delete_pod()
