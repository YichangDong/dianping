local voucherId = ARGV[1]
local userId = ARGV[2]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

if(redis.call('sismember', orderKey, userId) == 1) then
    redis.call('incr', stockKey)
    redis.call('srem', orderKey, userId)
end

return 0

