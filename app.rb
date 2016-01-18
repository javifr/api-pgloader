require 'rubygems'
require 'sinatra/base'
require 'sinatra/json'
require 'pry'

class Api < Sinatra::Base
    
    set :db, 'postgres://javi:@localhost:5432/loyal_guru?tablename='
    set :path, '/Users/javi/code/api-pgloader/tmp/'

    get '/api' do
      'This api is working mate!'
    end

    get '/api/load' do

      # Params
      table = params[:table] #activities
      company = params[:company] # canada
      url = params[:url] # http://amazon....
      fields = params[:fields] # id,product_type,description

      # Env Vars
      db_connect = "#{settings.db}#{company}.#{table}"
      filename = "#{company}.#{table}.#{Time.now.strftime("%Y%m%d%M%S")}.load"

      file_csv = `curl -o ./tmp/#{filename}.csv "#{url}"`

      # Creating load file
      contents = "LOAD CSV\n"+
         "FROM './#{filename}.csv'\n"+
              "HAVING FIELDS\n"+
              "(\n"+
               "#{fields}\n"+
              ")\n"+
         "INTO #{db_connect}\n"+
          "TARGET COLUMNS\n"+
          "(\n"+
          "#{fields}\n"+
          ")\n"+
          "WITH fields terminated by ';',\n"+
          "skip header = 1\n"+
      ";"

      # Writing load file
      out_file = File.new(settings.path+filename, "w")
      out_file.puts(contents)
      out_file.close

      # Calling pgloader file.load
      ret = `pgloader #{settings.path+filename}`

      remove_file = `rm tmp/#{filename}.csv`

      # Managing response
      if $?.success?
        # Write success log file
        out_file = File.new(settings.path+filename+".log", "w")
        out_file.puts(ret)
        out_file.close

        if ret.include? "Total import time"
          ret = ret.split("Total import time")
          ret = ret[1].gsub(/\s+/, ' ').split(" ")
          # status 200 response with json content
          json :read => ret[0], :imported => ret[1], :errors => ret[2]
        end

      else
        # Write error log file
        out_file = File.new(settings.path+filename+".error", "w")
        out_file.puts(ret)
        out_file.close

        # status 500 response indicating server error
        status 500
      end

    end

end
