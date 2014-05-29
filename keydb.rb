# encoding: UTF-8
require 'fiber'
require 'eventmachine'
require 'evma_httpserver'
require 'cgi'
require 'json'
require 'yaml'
require './adapters/localdb'
# require 'iconv'

# To read with a key called 'page1' from database 'pages'
# GET http://localhost:5029/pages/page1?API_KEY=5555555555
#
# To write a value 'writethis' to the key 'page1' from the database 'pages'
# POST http://localhost:5029/pages/page1?value=writethis&API_KEY=5555555555

class Handler  < EventMachine::Connection
  include EventMachine::HttpServer

  def self.format_response hsh, callback
  	response = JSON.generate(hsh)
  	if callback
  		response = "#{callback}( #{response} )"
  	end
  	response
  end

  def process_http_request
    resp = EventMachine::DelegatedHttpResponse.new( self )

    EM.next_tick do
   	# p @http_protocol
     # p @http_request_method
     # p @http_cookie
     # p @http_if_none_match
     # p @http_content_type
     # p @http_path_info
     # p @http_request_uri
     # p @http_query_string
     # p @http_post_content
     # p @http_headers

	    params = CGI.parse(@http_query_string || '')
	    params.merge!(CGI.parse(@http_post_content)) if @http_post_content

	    api_key = params['api_key'].first
	    params.delete('api_key')

  		# Supporting JSONP
  		callback = params['callback'].first
  		params.delete('callback')

  		resp.status = 200
  		resp.content_type 'application/json'

	    unless YAML::load_file('./config.yml')[:api_keys].any?{|k| k == api_key}
	    	resp.content = Handler.format_response({:error => "API key not valid"}, callback)
			  resp.send_response
	    	next
	    end

	    # Get REST parameters
  		rest_params = @http_path_info.split('/').delete_if(&:empty?)
  		db_name = CGI.unescape(rest_params[0])
  		obj_name = CGI.unescape(rest_params[1]) if rest_params[1]

	    if db_name.nil? || db_name.empty?
	    	resp.content = Handler.format_response({:error => "No DB selected"}, callback)
	    	resp.send_response
	    	next
	    end

	    if !params['command'].empty?
    		if params['command'].first == 'clear'
    			Fiber.new{
	    			COREDB.clear(db_name)
	    		}.resume
    			resp.content = Handler.format_response({:success => "Marked for deletion"}, callback)
    			resp.send_response
    		elsif params['command'].first == 'exists'
    			Fiber.new{
	    			if COREDB.exists?(db_name, obj_name)
	    				resp.content = Handler.format_response({:exists => true}, callback)
	    			else
	    				resp.content = Handler.format_response({:exists => false}, callback)
	    			end
    				resp.send_response
	    		}.resume
        elsif params['command'].first == 'count'
          Fiber.new{
            resp.content = Handler.format_response({:count => COREDB.count(db_name)}, callback)
            resp.send_response
          }.resume
    		end
	    else
        # Write
	    	if !params['value'].empty?
	    		puts "writing key #{obj_name} to db #{db_name}"
	    		Fiber.new{
            # puts "getting #{params['value'].first.encoding}"
	    			ret = COREDB.write(db_name, obj_name, params['value'].first)
	    			hsh = ret ? {:success => true} : {:error => COREDB.errors}
	    			resp.content = Handler.format_response(hsh, callback)
	    			resp.send_response
	    		}.resume
	    	else
          # Read
	    		if @http_request_method == 'GET'
	    			if obj_name
			    		puts "reading value #{obj_name} from #{db_name}"
			    		Fiber.new{
			    			result = COREDB.read(db_name, obj_name)
                # puts "throwing back #{result.encoding}"
                # result = result.encode('UTF-8', invalid: :replace, undef: :replace)
				    		resp.content = Handler.format_response({:value => result}, callback)
				    		resp.send_response
			    		}.resume
			    	end
		    	elsif @http_request_method == 'DELETE'
		    		puts "DELETE #{obj_name}"
		    		Fiber.new{
		    			COREDB.destroy(db_name, [obj_name])
		    			resp.content = Handler.format_response({:success => true}, callback)
		    			resp.send_response
		    		}.resume
		    	end
	    	end
	    end
	  end
  end
end

EventMachine::run {
	COREDB = LocalDB
	port = ARGV[0] || 5029
  EventMachine::start_server("0.0.0.0", port, Handler)
  puts "KeyDB - A basic key/value database that won't lose your data"
  puts "listening on port #{port}"
}
