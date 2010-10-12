class RedisCollector < Padrino::Application
  redis = Redis.new
  register Padrino::Mailer
  register Padrino::Helpers

  get :index do
    hosts = redis.smembers('hosts')
    hosts.to_json
  end

  get ':hostname' do
    plugins = redis.smembers("#{params[:hostname]}:plugins")
    plugins.to_json
  end
  
  get ':hostname/:plugin' do
    instances = redis.smembers("#{params[:hostname]}:#{params[:plugin]}:instances")
    instances.to_json
  end
  
  get ':hostname/:plugin/:instance' do
    types = redis.smembers("#{params[:hostname]}:#{params[:plugin]}:#{params[:instance]}:types")
    types.to_json
  end
  
  get ':hostname/:plugin/:instance/:dsname' do
    instances = redis.smembers("#{params[:hostname]}:#{params[:plugin]}:#{params[:instance]}:#{params[:dsname]}:instances")
    instances.to_json
  end
  
  get ':hostname/:plugin/:instance/:dsname/:dinstance' do
    sources = redis.smembers("#{params[:hostname]}:#{params[:plugin]}:#{params[:instance]}:#{params[:dsname]}:#{params[:dinstance]}:sources")
    sources.to_json
  end
  
  get ':hostname/:plugin/:instance/:dsname/:dinstance/:ds' do
    k = "#{params[:hostname]}:#{params[:plugin]}:#{params[:instance]}:#{params[:dsname]}:#{params[:dinstance]}:#{params[:ds]}:values"
    values = redis.zrevrange k,0,-1, :with_scores => true
    values.to_json
  end
    
  post :index do
    raw = request.body.read
    stats = JSON.parse(raw)
    stats.each do |stat|
      host = stat['host']
      plugin = stat['plugin']
      plugin_instance = stat['plugin_instance'] == "" ? 0 : stat['plugin_instance']
      type = stat['type']
      type_instance = stat['type_instance'] == "" ? 0 : stat['type_instance']
      
      redis.sadd "hosts", host
      redis.sadd "plugins", plugin
      redis.sadd "#{host}:plugins", plugin
      redis.sadd "#{host}:#{plugin}:instances", plugin_instance
      redis.sadd "#{host}:#{plugin}:interval", stat['interval']

      redis.sadd "#{host}:#{plugin}:#{plugin_instance}:types", type
      redis.sadd "#{host}:#{plugin}:#{plugin_instance}:#{type}:instances", type_instance
      stat['values'].each_index do |idx|
        dsname = stat['dsnames'][idx]
        dstype = stat['dstypes'][idx]
        value = stat['values'][idx]
        redis.sadd "#{host}:#{plugin}:#{plugin_instance}:#{type}:#{type_instance}:sources", dsname
        redis.sadd "#{host}:#{plugin}:#{plugin_instance}:#{type}:#{type_instance}:#{dsname}:type", dstype
        redis.zadd "#{host}:#{plugin}:#{plugin_instance}:#{type}:#{type_instance}:#{dsname}:values", stat['time'], value
      end
    end
    "OK"
  end  
end
