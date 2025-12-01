// 定义通用的KV存储接口
export interface IKVDatabase<T = any> {
  get(key: string, ttl?: number): Promise<T | null>;
  put(key: string, value: T): Promise<void>;
}

export function createCacheDecorator<T = any>(
  db: IKVDatabase<T>,
  default_ttl: number = 60, // 默认60秒
) {
  return function cache(ttl: number = default_ttl, prefix: string = '') {
    return function (
      target: any,
      property_key: string,
      descriptor: PropertyDescriptor,
    ) {
      const original_method = descriptor.value;

      descriptor.value = async function (...args: any[]): Promise<T> {
        try {
          const cache_key = `${prefix}:${property_key}:${JSON.stringify(
            args,
          )}`.slice(0, 255);

          // 使用通用的KV存储接口
          const cached = await db.get(cache_key, ttl);
          if (cached !== null) {
            return cached;
          }

          const result = await original_method.apply(this, args);
          await db.put(cache_key, result);
          return result;
        } catch (error) {
          // 如果缓存操作失败，直接执行原始方法
          return original_method.apply(this, args);
        }
      };

      return descriptor;
    };
  };
}
