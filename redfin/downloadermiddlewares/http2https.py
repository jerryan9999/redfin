
class HttpschangeMiddleware(object):

  def process_request(self, request, spider):
    if request.url.startswith("http://"):
      https_url = request.url.replace("http://","https://")
      r= request.replace(url=https_url)
      return  r
