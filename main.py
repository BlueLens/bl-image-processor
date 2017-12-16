from __future__ import print_function

import os
import time
from multiprocessing import Process
from threading import Timer
import urllib.request
from PIL import Image
import pickle
import uuid
from bluelens_spawning_pool import spawning_pool
from stylelens_product import Product
from stylelens_product import Object
from stylelens_product import ProductApi
from stylelens_product import ObjectApi
from stylelens_product.rest import ApiException
from util import s3
import redis

from bluelens_log import Logging


AWS_OBJ_IMAGE_BUCKET = 'bluelens-style-object'
AWS_MOBILE_IMAGE_BUCKET = 'bluelens-style-mainimage'

OBJECT_IMAGE_WIDTH = 300
OBJECT_IMAGE_HEITH = 300
MOBILE_FULL_WIDTH = 375
MOBILE_THUMBNAIL_WIDTH = 200

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
REDIS_PRODUCT_HASH = 'bl:product:hash'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-image-processor')
product_api = ProductApi()
object_api = ObjectApi()
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

storage = s3.S3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

heart_bit = True

def process_image(p_data):
  log.info('process_image')
  product = pickle.loads(p_data)

  make_mobile_images(product)

  # save_main_image_as_object(product)

def make_mobile_image(image_name, type, image_path):

  if type == 'full':
    basewidth = MOBILE_FULL_WIDTH
  else:
    basewidth = MOBILE_THUMBNAIL_WIDTH

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
    log.error('url:' + image_path)
    log.error(str(e))
    delete_pod()

def make_mobile_images(product_dic):
  full_image = make_mobile_image(product_dic['id'], 'full', product_dic['main_image'])
  if full_image == None:
    return
  thumb_image = make_mobile_image(product_dic['id'], 'thumb', product_dic['main_image'])
  if thumb_image == None:
    return

  sub_images = []
  for sub_img in product_dic['sub_images']:
    sub_image = make_mobile_image(str(uuid.uuid4()), 'sub', sub_img)
    sub_images.append(sub_image)

  product = Product()
  product.id = product_dic['id']
  product.main_image_mobile_full = full_image
  product.main_image_mobile_thumb = thumb_image
  product.sub_images_mobile = sub_images
  update_product_to_db(product)

  product_dic['main_image_mobile_full'] = full_image
  product_dic['main_image_mobile_thumb'] = thumb_image
  product_dic['sub_images_mobile'] = sub_images
  rconn.lpush(REDIS_PRODUCT_CLASSIFY_QUEUE, pickle.dumps(product_dic))
  rconn.hset(REDIS_PRODUCT_HASH, product.id, pickle.dumps(product_dic))

def save_mobile_image_to_storage(name, path):
  log.debug('save_mobile_image_to_storage')
  file = name + '.jpg'
  key = os.path.join(RELEASE_MODE, 'mobile', path, name + '.jpg')
  is_public = True
  file_url = storage.upload_file_to_bucket(AWS_MOBILE_IMAGE_BUCKET, file, key, is_public=is_public)
  log.info(file_url)
  return file_url

def save_main_image_as_object(product):
  log.info('save_main_image_as_object')
  image_path = product['main_image']
  try:
    f = urllib.request.urlopen(image_path)
  except Exception as e:
    log.error(str(e))
    return
  im = Image.open(f).convert('RGB')
  size = OBJECT_IMAGE_WIDTH, OBJECT_IMAGE_HEITH
  im.thumbnail(size, Image.ANTIALIAS)

  object = Object()
  object.product_id = product['id']
  object.storage = 's3'
  object.bucket = AWS_OBJ_IMAGE_BUCKET
  object.class_code = '0'
  id = save_object_to_db(object)
  im.save(id + '.jpg')

  object.name = id
  save_to_storage(object)
  push_object_to_queue(object)

def push_object_to_queue(obj):
  log.info('push_object_to_queue')
  rconn.lpush(REDIS_OBJECT_INDEX_QUEUE, pickle.dumps(obj.to_dict(), protocol=2))

def save_object_to_db(obj):
  log.info('save_object_to_db')
  try:
    api_response = object_api.add_object(obj)
    log.debug(api_response)
  except ApiException as e:
    log.error("Exception when calling ObjectApi->add_object: %s\n" % e)

  return api_response.data.object_id

def update_product_to_db(product):
  log.debug('update_product_to_db')
  try:
    log.debug('product_api host:' + product_api.api_client.host)
    log.debug(product)
    api_response = product_api.update_product_by_id(product.id, product)
    log.debug(api_response)
  except ApiException as e:
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

def save_to_storage(obj):
  log.debug('save_to_storage')
  file = obj.name + '.jpg'
  key = os.path.join(RELEASE_MODE, obj.class_code, obj.name + '.jpg')
  is_public = True
  storage.upload_file_to_bucket(AWS_OBJ_IMAGE_BUCKET, file, key, is_public=is_public)
  log.debug('save_to_storage done')

def dispatch_job(rconn):
  log.info('Start dispatch_job')
  Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  while True:
    key, value = rconn.blpop([REDIS_PRODUCT_IMAGE_PROCESS_QUEUE])
    start_time = time.time()
    process_image(value)
    elapsed_time = time.time() - start_time
    log.info('image-processing time: ' + str(elapsed_time))
    global  heart_bit
    heart_bit = True

if __name__ == '__main__':
  log.info('Start bl-image-processor')
  try:
    Process(target=dispatch_job, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
    delete_pod()
