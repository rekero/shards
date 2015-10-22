#!/usr/bin/env ruby

require 'mysql2'

@dbConn = Mysql2::Client.new(host: 'localhost', username: 'root', database: 'por')
threads_count = (ARGV[0] || 1).to_i
threads = []
first_not_null =  @dbConn.query("select * from users where shard_id is NULL LIMIT 1;").first
first_element = first_not_null['id'] if first_not_null
last_element = @dbConn.query("select max(id) from users;").first['max(id)']
start_time = Time.now.to_i
if first_not_null
  (0..threads_count-1).each do |i|
    threads << Thread.new {
      db_conn = Mysql2::Client.new(host: 'localhost', username: 'root', database: 'por')
      p "Thread #{i} start working"
      j = (first_element / 1000) + i
      while j <= last_element / 1000
         p "Estimated time is #{(Time.now.to_i - start_time) * (last_element - j*1000) / (j*1000 - first_element)} s" if i == 0
         begin
           result = db_conn.query("update users set shard_id = FLOOR( 1 + RAND( ) * 10 ) where id between #{j}*1000 and #{j+1}*1000;")
         rescue => e
           errors = e.message
         end
         p errors if errors
         j += threads_count
      end
    }
  end
  threads.each {|t| t.join() }
end
