
class HttpschangeMiddleware(object):

  def process_request(self, request, spider):
    https_new = request.url.replace("http://","https://").replace('status=9','status=-1').replace('status=1','status=-1').replace('num_homes=350','num_homes=35000')
    r= request.replace(url=https_new)
    return  r
