-- seckill_rollback.lua
local voucherId = ARGV[1]
local userId = ARGV[2]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- 库存加回去
redis.call('incrby', stockKey, 1)
-- 用户从已下单集合里移除
redis.call('srem', orderKey, userId)

return 0