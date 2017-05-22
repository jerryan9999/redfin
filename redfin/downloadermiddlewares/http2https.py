
class HttpschangeMiddleware(object):

  def process_request(self, request, spider):
    if request.url.startswith("http://"):
      https_url = request.url.replace("http://","https://")
      https_url.replace('status=9','status=-1').replace('status=1','status=-1').replace('num_homes=350','num_homes=35000')
      r= request.replace(url=https_url)
      return  r
