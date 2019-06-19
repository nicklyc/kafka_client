/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A convenient base class for configurations to extend.
 * <p>
 * This class holds both the original configuration that was provided as well as the parsed
 */
public class AbstractConfig {

    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * 已经使用的配置
     */
    private final Set<String> used;

    /**
     * 用户自定义的原始配置
     */
    private final Map<String, ?> originals;

    /**
     * 解析的值，用户自定义配置和初始化配置，
     * 如果用户自定义的配置包含默认配置
     * 这个时候会解析的时候将默认值替换为用户自定义值
     */
    private final Map<String, Object> values;
    /**
     * kafka默认配置
     */
    private final ConfigDef definition;

    /**
     * 解析配置
     *
     * @param definition 默认配置
     * @param originals  自定义配置
     * @param doLog
     */
    public AbstractConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        //校验配置key的类型
        for (Map.Entry<?, ?> entry : originals.entrySet())
            if (!(entry.getKey() instanceof String))
                throw new ConfigException(entry.getKey().toString(), entry.getValue(), "Key must be a string.");
        this.originals = (Map<String, ?>) originals;

        //解析用户配置到values中
        this.values = definition.parse(this.originals);
        Map<String, Object> configUpdates = postProcessParsedConfig(Collections.unmodifiableMap(this.values));
        for (Map.Entry<String, Object> update : configUpdates.entrySet()) {
            this.values.put(update.getKey(), update.getValue());
        }
        //解析默认配置到values中，
        definition.parse(this.values);

        //定义使用的配置
        this.used = Collections.synchronizedSet(new HashSet<String>());

        this.definition = definition;
        if (doLog)
            logAll();
    }

    public AbstractConfig(ConfigDef definition, Map<?, ?> originals) {
        this(definition, originals, true);
    }

    /**
     * Called directly after user configs got parsed (and thus default values got set).
     * This allows to change default values for "secondary defaults" if required.
     *
     * @param parsedValues unmodifiable map of current configuration
     * @return a map of updates that should be applied to the configuration (will be validated to prevent bad updates)
     */
    protected Map<String, Object> postProcessParsedConfig(Map<String, Object> parsedValues) {
        return Collections.emptyMap();
    }

    protected Object get(String key) {
        if (!values.containsKey(key))
            throw new ConfigException(String.format("Unknown configuration '%s'", key));
        //添加到使用配置
        used.add(key);
        //从已经解析配置中获配置值
        return values.get(key);
    }

    public void ignore(String key) {
        used.add(key);
    }

    public Short getShort(String key) {
        return (Short) get(key);
    }

    public Integer getInt(String key) {
        return (Integer) get(key);
    }

    public Long getLong(String key) {
        return (Long) get(key);
    }

    public Double getDouble(String key) {
        return (Double) get(key);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key) {
        return (List<String>) get(key);
    }

    public Boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public ConfigDef.Type typeOf(String key) {
        ConfigDef.ConfigKey configKey = definition.configKeys().get(key);
        if (configKey == null)
            return null;
        return configKey.type;
    }

    public Password getPassword(String key) {
        return (Password) get(key);
    }

    public Class<?> getClass(String key) {
        return (Class<?>) get(key);
    }

    public Set<String> unused() {
        Set<String> keys = new HashSet<>(originals.keySet());
        keys.removeAll(used);
        return keys;
    }

    /**
     * 将用户自定义的配置  转变成map
     *
     * @return
     */
    public Map<String, Object> originals() {
        Map<String, Object> copy = new RecordingMap<>();
        copy.putAll(originals);
        return copy;
    }

    /**
     * Get all the original settings, ensuring that all values are of type String.
     *
     * @return the original settings
     * @throws ClassCastException if any of the values are not strings
     */
    public Map<String, String> originalsStrings() {
        Map<String, String> copy = new RecordingMap<>();
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (!(entry.getValue() instanceof String))
                throw new ClassCastException("Non-string value found in original settings for key " + entry.getKey() +
                        ": " + (entry.getValue() == null ? null : entry.getValue().getClass().getName()));
            copy.put(entry.getKey(), (String) entry.getValue());
        }
        return copy;
    }

    /**
     * Gets all original settings with the given prefix, stripping the prefix before adding it to the output.
     *
     * @param prefix the prefix to use as a filter
     * @return a Map containing the settings with the prefix
     */
    public Map<String, Object> originalsWithPrefix(String prefix) {
        return originalsWithPrefix(prefix, true);
    }

    /**
     * Gets all original settings with the given prefix.
     *
     * @param prefix the prefix to use as a filter
     * @param strip  strip the prefix before adding to the output if set true
     * @return a Map containing the settings with the prefix
     */
    public Map<String, Object> originalsWithPrefix(String prefix, boolean strip) {
        Map<String, Object> result = new RecordingMap<>(prefix, false);
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                if (strip)
                    result.put(entry.getKey().substring(prefix.length()), entry.getValue());
                else
                    result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Put all keys that do not start with {@code prefix} and their parsed values in the result map and then
     * put all the remaining keys with the prefix stripped and their parsed values in the result map.
     * <p>
     * This is useful if one wants to allow prefixed configs to override default ones.
     * <p>
     * Two forms of prefixes are supported:
     * <ul>
     * <li>listener.name.{listenerName}.some.prop: If the provided prefix is `listener.name.{listenerName}.`,
     * the key `some.prop` with the value parsed using the definition of `some.prop` is returned.</li>
     * <li>listener.name.{listenerName}.{mechanism}.some.prop: If the provided prefix is `listener.name.{listenerName}.`,
     * the key `{mechanism}.some.prop` with the value parsed using the definition of `some.prop` is returned.
     * This is used to provide per-mechanism configs for a broker listener (e.g sasl.jaas.config)</li>
     * </ul>
     * </p>
     */
    public Map<String, Object> valuesWithPrefixOverride(String prefix) {
        Map<String, Object> result = new RecordingMap<>(values(), prefix, true);
        for (Map.Entry<String, ?> entry : originals.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                String keyWithNoPrefix = entry.getKey().substring(prefix.length());
                ConfigDef.ConfigKey configKey = definition.configKeys().get(keyWithNoPrefix);
                if (configKey != null)
                    result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                else {
                    String keyWithNoSecondaryPrefix = keyWithNoPrefix.substring(keyWithNoPrefix.indexOf('.') + 1);
                    configKey = definition.configKeys().get(keyWithNoSecondaryPrefix);
                    if (configKey != null)
                        result.put(keyWithNoPrefix, definition.parseValue(configKey, entry.getValue(), true));
                }
            }
        }
        return result;
    }

    /**
     * If at least one key with {@code prefix} exists, all prefixed values will be parsed and put into map.
     * If no value with {@code prefix} exists all unprefixed values will be returned.
     * <p>
     * This is useful if one wants to allow prefixed configs to override default ones, but wants to use either
     * only prefixed configs or only regular configs, but not mix them.
     */
    public Map<String, Object> valuesWithPrefixAllOrNothing(String prefix) {
        Map<String, Object> withPrefix = originalsWithPrefix(prefix, true);

        if (withPrefix.isEmpty()) {
            return new RecordingMap<>(values(), "", true);
        } else {
            Map<String, Object> result = new RecordingMap<>(prefix, true);

            for (Map.Entry<String, ?> entry : withPrefix.entrySet()) {
                ConfigDef.ConfigKey configKey = definition.configKeys().get(entry.getKey());
                if (configKey != null)
                    result.put(entry.getKey(), definition.parseValue(configKey, entry.getValue(), true));
            }

            return result;
        }
    }

    public Map<String, ?> values() {
        return new RecordingMap<>(values);
    }

    private void logAll() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);

        for (Map.Entry<String, Object> entry : new TreeMap<>(this.values).entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }

    /**
     * Log warnings for any unused configurations
     */
    public void logUnused() {
        for (String key : unused())
            log.warn("The configuration '{}' was supplied but isn't a known config.", key);
    }

    /**
     * 获取T接口的的配置实例对象
     *
     * @param key 配置ksy
     * @param t   接口
     * @return A    配置的value实例
     */
    public <T> T getConfiguredInstance(String key, Class<T> t) {
        //获取配置的Class对象
        Class<?> c = getClass(key);
        if (c == null)
            return null;
        //反射创建实例
        Object o = Utils.newInstance(c);
        //类型判断
        if (!t.isInstance(o))
            throw new KafkaException(c.getName() + " is not an instance of " + t.getName());
        if (o instanceof Configurable)
            //配置到用户自定义配置
            ((Configurable) o).configure(originals());
        return t.cast(o);
    }

    /**
     * 获取T接口的的配置实例对象，配置了多个实现类名字
     *
     * @param key 配置名
     * @param t   接口
     * @return 实例集合
     */
    public <T> List<T> getConfiguredInstances(String key, Class<T> t) {
        return getConfiguredInstances(key, t, Collections.<String, Object>emptyMap());
    }

    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     *
     * @param key             The configuration key for the class
     * @param t               The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(String key, Class<T> t, Map<String, Object> configOverrides) {
        return getConfiguredInstances(getList(key), t, configOverrides);
    }


    /**
     * Get a list of configured instances of the given class specified by the given configuration key. The configuration
     * may specify either null or an empty string to indicate no configured instances. In both cases, this method
     * returns an empty list to indicate no configured instances.
     *
     * @param classNames      The list of class names of the instances to create
     * @param t               The interface the class should implement
     * @param configOverrides Configuration overrides to use.
     * @return The list of configured instances
     */
    public <T> List<T> getConfiguredInstances(List<String> classNames, Class<T> t, Map<String, Object> configOverrides) {
        List<T> objects = new ArrayList<T>();
        if (classNames == null)
            return objects;
        Map<String, Object> configPairs = originals();
        configPairs.putAll(configOverrides);
        for (Object klass : classNames) {
            Object o;
            if (klass instanceof String) {
                try {
                    o = Utils.newInstance((String) klass, t);
                } catch (ClassNotFoundException e) {
                    throw new KafkaException(klass + " ClassNotFoundException exception occurred", e);
                }
            } else if (klass instanceof Class<?>) {
                o = Utils.newInstance((Class<?>) klass);
            } else
                throw new KafkaException("List contains element of type " + klass.getClass().getName() + ", expected String or Class");
            if (!t.isInstance(o))
                throw new KafkaException(klass + " is not an instance of " + t.getName());
            if (o instanceof Configurable)
                ((Configurable) o).configure(configPairs);
            objects.add(t.cast(o));
        }
        return objects;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractConfig that = (AbstractConfig) o;

        return originals.equals(that.originals);
    }

    @Override
    public int hashCode() {
        return originals.hashCode();
    }

    /**
     * Marks keys retrieved via `get` as used. This is needed because `Configurable.configure` takes a `Map` instead
     * of an `AbstractConfig` and we can't change that without breaking public API like `Partitioner`.
     */
    private class RecordingMap<V> extends HashMap<String, V> {

        private final String prefix;
        private final boolean withIgnoreFallback;

        RecordingMap() {
            this("", false);
        }

        RecordingMap(String prefix, boolean withIgnoreFallback) {
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        RecordingMap(Map<String, ? extends V> m) {
            this(m, "", false);
        }

        RecordingMap(Map<String, ? extends V> m, String prefix, boolean withIgnoreFallback) {
            super(m);
            this.prefix = prefix;
            this.withIgnoreFallback = withIgnoreFallback;
        }

        @Override
        public V get(Object key) {
            if (key instanceof String) {
                String stringKey = (String) key;
                String keyWithPrefix;
                if (prefix.isEmpty()) {
                    keyWithPrefix = stringKey;
                } else {
                    keyWithPrefix = prefix + stringKey;
                }
                ignore(keyWithPrefix);
                if (withIgnoreFallback)
                    ignore(stringKey);
            }
            return super.get(key);
        }
    }
}
