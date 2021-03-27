package com.alibaba.otter.canal.client.adapter.config.bind;

import java.beans.PropertyEditor;
import java.net.InetAddress;
import java.util.*;

import org.springframework.beans.*;
import org.springframework.beans.propertyeditors.FileEditor;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.validation.AbstractPropertyBindingResult;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.DataBinder;

/**
 * Binder implementation that allows caller to bind to maps and also allows
 * property names to match a bit loosely (if underscores or dashes are removed
 * and replaced with camel case for example).
 *
 * @author Dave Syer
 * @author Phillip Webb
 * @author Stephane Nicoll
 * @author Andy Wilkinson
 * @see RelaxedNames
 */
public class RelaxedDataBinder extends DataBinder {

    private static final Set<Class<?>> EXCLUDED_EDITORS;

    static {
        Set<Class<?>> excluded = new HashSet<>();
        excluded.add(FileEditor.class);
        EXCLUDED_EDITORS = Collections.unmodifiableSet(excluded);
    }

    private static final Object           BLANK       = new Object();

    private String                        namePrefix;

    private boolean                       ignoreNestedProperties;

    private MultiValueMap<String, String> nameAliases = new LinkedMultiValueMap<>();

    /**
     * Create a new {@link RelaxedDataBinder} instance.
     *
     * @param target the target into which properties are bound
     */
    public RelaxedDataBinder(Object target){
        super(wrapTarget(target));
    }

    /**
     * Create a new {@link RelaxedDataBinder} instance.
     *
     * @param target the target into which properties are bound
     * @param namePrefix An optional prefix to be used when reading properties
     */
    public RelaxedDataBinder(Object target, String namePrefix){
        super(wrapTarget(target), (StringUtils.hasLength(namePrefix) ? namePrefix : DEFAULT_OBJECT_NAME));
        this.namePrefix = cleanNamePrefix(namePrefix);
    }

    private String cleanNamePrefix(String namePrefix) {
        if (!StringUtils.hasLength(namePrefix)) {
            return null;
        }
        return (namePrefix.endsWith(".") ? namePrefix : namePrefix + ".");
    }

    /**
     * Flag to disable binding of nested properties (i.e. those with period
     * separators in their paths). Can be useful to disable this if the name prefix
     * is empty and you don't want to ignore unknown fields.
     *
     * @param ignoreNestedProperties the flag to set (default false)
     */
    public void setIgnoreNestedProperties(boolean ignoreNestedProperties) {
        this.ignoreNestedProperties = ignoreNestedProperties;
    }

    /**
     * Set name aliases.
     *
     * @param aliases a map of property name to aliases
     */
    public void setNameAliases(Map<String, List<String>> aliases) {
        this.nameAliases = new LinkedMultiValueMap<>(aliases);
    }

    /**
     * Add aliases to the {@link DataBinder}.
     *
     * @param name the property name to alias
     * @param alias aliases for the property names
     * @return this instance
     */
    public RelaxedDataBinder withAlias(String name, String... alias) {
        for (String value : alias) {
            this.nameAliases.add(name, value);
        }
        return this;
    }

    @Override
    protected void doBind(MutablePropertyValues propertyValues) {
        super.doBind(modifyProperties(propertyValues, getTarget()));
    }

    /**
     * Modify the property values so that period separated property paths are valid
     * for map keys. Also creates new maps for properties of map type that are null
     * (assuming all maps are potentially nested). The standard bracket {@code[...]}
     * dereferencing is also accepted.
     *
     * @param propertyValues the property values
     * @param target the target object
     * @return modified property values
     */
    private MutablePropertyValues modifyProperties(MutablePropertyValues propertyValues, Object target) {
        propertyValues = getPropertyValuesForNamePrefix(propertyValues);
        if (target instanceof RelaxedDataBinder.MapHolder) {
            propertyValues = addMapPrefix(propertyValues);
        }
        BeanWrapper wrapper = new BeanWrapperImpl(target);
        wrapper.setConversionService(new RelaxedConversionService(getConversionService()));
        wrapper.setAutoGrowNestedPaths(true);
        List<PropertyValue> sortedValues = new ArrayList<>();
        Set<String> modifiedNames = new HashSet<>();
        List<String> sortedNames = getSortedPropertyNames(propertyValues);
        for (String name : sortedNames) {
            PropertyValue propertyValue = propertyValues.getPropertyValue(name);
            PropertyValue modifiedProperty = modifyProperty(wrapper, propertyValue);
            if (modifiedNames.add(modifiedProperty.getName())) {
                sortedValues.add(modifiedProperty);
            }
        }
        return new MutablePropertyValues(sortedValues);
    }

    private List<String> getSortedPropertyNames(MutablePropertyValues propertyValues) {
        List<String> names = new LinkedList<>();
        for (PropertyValue propertyValue : propertyValues.getPropertyValueList()) {
            names.add(propertyValue.getName());
        }
        sortPropertyNames(names);
        return names;
    }

    /**
     * Sort by name so that parent properties get processed first (e.g. 'foo.bar'
     * before 'foo.bar.spam'). Don't use Collections.sort() because the order might
     * be significant for other property names (it shouldn't be but who knows what
     * people might be relying on, e.g. HSQL has a JDBCXADataSource where
     * "databaseName" is a synonym for "url").
     *
     * @param names the names to sort
     */
    private void sortPropertyNames(List<String> names) {
        for (String name : new ArrayList<>(names)) {
            int propertyIndex = names.indexOf(name);
            RelaxedDataBinder.BeanPath path = new RelaxedDataBinder.BeanPath(name);
            for (String prefix : path.prefixes()) {
                int prefixIndex = names.indexOf(prefix);
                if (prefixIndex >= propertyIndex) {
                    // The child property has a parent in the list in the wrong order
                    names.remove(name);
                    names.add(prefixIndex, name);
                }
            }
        }
    }

    private MutablePropertyValues addMapPrefix(MutablePropertyValues propertyValues) {
        MutablePropertyValues rtn = new MutablePropertyValues();
        for (PropertyValue pv : propertyValues.getPropertyValues()) {
            rtn.add("map." + pv.getName(), pv.getValue());
        }
        return rtn;
    }

    private MutablePropertyValues getPropertyValuesForNamePrefix(MutablePropertyValues propertyValues) {
        if (!StringUtils.hasText(this.namePrefix) && !this.ignoreNestedProperties) {
            return propertyValues;
        }
        MutablePropertyValues rtn = new MutablePropertyValues();
        for (PropertyValue value : propertyValues.getPropertyValues()) {
            String name = value.getName();
            for (String prefix : new RelaxedNames(stripLastDot(this.namePrefix))) {
                for (String separator : new String[] { ".", "_" }) {
                    String candidate = (StringUtils.hasLength(prefix) ? prefix + separator : prefix);
                    if (name.startsWith(candidate)) {
                        name = name.substring(candidate.length());
                        if (!(this.ignoreNestedProperties && name.contains("."))) {
                            PropertyOrigin propertyOrigin = OriginCapablePropertyValue.getOrigin(value);
                            rtn.addPropertyValue(
                                new OriginCapablePropertyValue(name, value.getValue(), propertyOrigin));
                        }
                    }
                }
            }
        }
        return rtn;
    }

    private String stripLastDot(String string) {
        if (StringUtils.hasLength(string) && string.endsWith(".")) {
            string = string.substring(0, string.length() - 1);
        }
        return string;
    }

    private PropertyValue modifyProperty(BeanWrapper target, PropertyValue propertyValue) {
        String name = propertyValue.getName();
        String normalizedName = normalizePath(target, name);
        if (!normalizedName.equals(name)) {
            return new PropertyValue(normalizedName, propertyValue.getValue());
        }
        return propertyValue;
    }

    /**
     * Normalize a bean property path to a format understood by a BeanWrapper. This
     * is used so that
     * <ul>
     * <li>Fuzzy matching can be employed for bean property names</li>
     * <li>Period separators can be used instead of indexing ([...]) for map
     * keys</li>
     * </ul>
     *
     * @param wrapper a bean wrapper for the object to bind
     * @param path the bean path to bind
     * @return a transformed path with correct bean wrapper syntax
     */
    protected String normalizePath(BeanWrapper wrapper, String path) {
        return initializePath(wrapper, new RelaxedDataBinder.BeanPath(path), 0);
    }

    @Override
    protected AbstractPropertyBindingResult createBeanPropertyBindingResult() {
        return new RelaxedDataBinder.RelaxedBeanPropertyBindingResult(getTarget(),
            getObjectName(),
            isAutoGrowNestedPaths(),
            getAutoGrowCollectionLimit(),
            getConversionService());
    }

    private String initializePath(BeanWrapper wrapper, RelaxedDataBinder.BeanPath path, int index) {
        String prefix = path.prefix(index);
        String key = path.name(index);
        if (path.isProperty(index)) {
            key = getActualPropertyName(wrapper, prefix, key);
            path.rename(index, key);
        }
        if (path.name(++index) == null) {
            return path.toString();
        }
        String name = path.prefix(index);
        TypeDescriptor descriptor = wrapper.getPropertyTypeDescriptor(name);
        if (descriptor == null || descriptor.isMap()) {
            if (isMapValueStringType(descriptor) || isBlanked(wrapper, name, path.name(index))) {
                path.collapseKeys(index);
            }
            path.mapIndex(index);
            extendMapIfNecessary(wrapper, path, index);
        } else if (descriptor.isCollection()) {
            extendCollectionIfNecessary(wrapper, path, index);
        } else if (descriptor.getType().equals(Object.class)) {
            if (isBlanked(wrapper, name, path.name(index))) {
                path.collapseKeys(index);
            }
            path.mapIndex(index);
            if (path.isLastNode(index)) {
                wrapper.setPropertyValue(path.toString(), BLANK);
            } else {
                String next = path.prefix(index + 1);
                if (wrapper.getPropertyValue(next) == null) {
                    wrapper.setPropertyValue(next, new LinkedHashMap<String, Object>());
                }
            }
        }
        return initializePath(wrapper, path, index);
    }

    private boolean isMapValueStringType(TypeDescriptor descriptor) {
        if (descriptor == null || descriptor.getMapValueTypeDescriptor() == null) {
            return false;
        }
        if (Properties.class.isAssignableFrom(descriptor.getObjectType())) {
            // Properties is declared as Map<Object,Object> but we know it's really
            // Map<String,String>
            return true;
        }
        Class<?> valueType = descriptor.getMapValueTypeDescriptor().getObjectType();
        return (valueType != null && CharSequence.class.isAssignableFrom(valueType));
    }

    @SuppressWarnings("rawtypes")
    private boolean isBlanked(BeanWrapper wrapper, String propertyName, String key) {
        Object value = (wrapper.isReadableProperty(propertyName) ? wrapper.getPropertyValue(propertyName) : null);
        if (value instanceof Map) {
            if (((Map) value).get(key) == BLANK) {
                return true;
            }
        }
        return false;
    }

    private void extendCollectionIfNecessary(BeanWrapper wrapper, RelaxedDataBinder.BeanPath path, int index) {
        String name = path.prefix(index);
        TypeDescriptor elementDescriptor = wrapper.getPropertyTypeDescriptor(name).getElementTypeDescriptor();
        if (!elementDescriptor.isMap() && !elementDescriptor.isCollection()
            && !elementDescriptor.getType().equals(Object.class)) {
            return;
        }
        Object extend = new LinkedHashMap<String, Object>();
        if (!elementDescriptor.isMap() && path.isArrayIndex(index)) {
            extend = new ArrayList<>();
        }
        wrapper.setPropertyValue(path.prefix(index + 1), extend);
    }

    private void extendMapIfNecessary(BeanWrapper wrapper, RelaxedDataBinder.BeanPath path, int index) {
        String name = path.prefix(index);
        TypeDescriptor parent = wrapper.getPropertyTypeDescriptor(name);
        if (parent == null) {
            return;
        }
        TypeDescriptor descriptor = parent.getMapValueTypeDescriptor();
        if (descriptor == null) {
            descriptor = TypeDescriptor.valueOf(Object.class);
        }
        if (!descriptor.isMap() && !descriptor.isCollection() && !descriptor.getType().equals(Object.class)) {
            return;
        }
        String extensionName = path.prefix(index + 1);
        if (wrapper.isReadableProperty(extensionName)) {
            Object currentValue = wrapper.getPropertyValue(extensionName);
            if ((descriptor.isCollection() && currentValue instanceof Collection)
                || (!descriptor.isCollection() && currentValue instanceof Map)) {
                return;
            }
        }
        Object extend = new LinkedHashMap<String, Object>();
        if (descriptor.isCollection()) {
            extend = new ArrayList<>();
        }
        if (descriptor.getType().equals(Object.class) && path.isLastNode(index)) {
            extend = BLANK;
        }
        wrapper.setPropertyValue(extensionName, extend);
    }

    private String getActualPropertyName(BeanWrapper target, String prefix, String name) {
        String propertyName = resolvePropertyName(target, prefix, name);
        if (propertyName == null) {
            propertyName = resolveNestedPropertyName(target, prefix, name);
        }
        return (propertyName != null ? propertyName : name);
    }

    private String resolveNestedPropertyName(BeanWrapper target, String prefix, String name) {
        StringBuilder candidate = new StringBuilder();
        for (String field : name.split("[_\\-\\.]")) {
            candidate.append(candidate.length() > 0 ? "." : "");
            candidate.append(field);
            String nested = resolvePropertyName(target, prefix, candidate.toString());
            if (nested != null) {
                Class<?> type = target.getPropertyType(nested);
                if ((type != null) && Map.class.isAssignableFrom(type)) {
                    // Special case for map property (gh-3836).
                    return nested + "[" + name.substring(candidate.length() + 1) + "]";
                }
                String propertyName = resolvePropertyName(target,
                    joinString(prefix, nested),
                    name.substring(candidate.length() + 1));
                if (propertyName != null) {
                    return joinString(nested, propertyName);
                }
            }
        }
        return null;
    }

    private String resolvePropertyName(BeanWrapper target, String prefix, String name) {
        Iterable<String> names = getNameAndAliases(name);
        for (String nameOrAlias : names) {
            for (String candidate : new RelaxedNames(nameOrAlias)) {
                try {
                    if (target.getPropertyType(joinString(prefix, candidate)) != null) {
                        return candidate;
                    }
                } catch (InvalidPropertyException ex) {
                    // swallow and continue
                }
            }
        }
        return null;
    }

    private String joinString(String prefix, String name) {
        return (StringUtils.hasLength(prefix) ? prefix + "." + name : name);
    }

    private Iterable<String> getNameAndAliases(String name) {
        List<String> aliases = this.nameAliases.get(name);
        if (aliases == null) {
            return Collections.singleton(name);
        }
        List<String> nameAndAliases = new ArrayList<>(aliases.size() + 1);
        nameAndAliases.add(name);
        nameAndAliases.addAll(aliases);
        return nameAndAliases;
    }

    private static Object wrapTarget(Object target) {
        if (target instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) target;
            target = new RelaxedDataBinder.MapHolder(map);
        }
        return target;
    }

    @Override
    public void registerCustomEditor(Class<?> requiredType, PropertyEditor propertyEditor) {
        if (propertyEditor == null || !EXCLUDED_EDITORS.contains(propertyEditor.getClass())) {
            super.registerCustomEditor(requiredType, propertyEditor);
        }
    }

    @Override
    public void registerCustomEditor(Class<?> requiredType, String field, PropertyEditor propertyEditor) {
        if (propertyEditor == null || !EXCLUDED_EDITORS.contains(propertyEditor.getClass())) {
            super.registerCustomEditor(requiredType, field, propertyEditor);
        }
    }

    /**
     * Holder to allow Map targets to be bound.
     */
    static class MapHolder {

        private Map<String, Object> map;

        MapHolder(Map<String, Object> map){
            this.map = map;
        }

        public void setMap(Map<String, Object> map) {
            this.map = map;
        }

        public Map<String, Object> getMap() {
            return this.map;
        }

    }

    /**
     * A path though properties of a bean.
     */
    private static class BeanPath {

        private List<PathNode> nodes;

        BeanPath(String path){
            this.nodes = splitPath(path);
        }

        public List<String> prefixes() {
            List<String> prefixes = new ArrayList<>();
            for (int index = 1; index < this.nodes.size(); index++) {
                prefixes.add(prefix(index));
            }
            return prefixes;
        }

        public boolean isLastNode(int index) {
            return index >= this.nodes.size() - 1;
        }

        private List<PathNode> splitPath(String path) {
            List<PathNode> nodes = new ArrayList<>();
            String current = extractIndexedPaths(path, nodes);
            for (String name : StringUtils.delimitedListToStringArray(current, ".")) {
                if (StringUtils.hasText(name)) {
                    nodes.add(new RelaxedDataBinder.BeanPath.PropertyNode(name));
                }
            }
            return nodes;
        }

        private String extractIndexedPaths(String path, List<PathNode> nodes) {
            int startRef = path.indexOf("[");
            String current = path;
            while (startRef >= 0) {
                if (startRef > 0) {
                    nodes.addAll(splitPath(current.substring(0, startRef)));
                }
                int endRef = current.indexOf("]", startRef);
                if (endRef > 0) {
                    String sub = current.substring(startRef + 1, endRef);
                    if (sub.matches("[0-9]+")) {
                        nodes.add(new RelaxedDataBinder.BeanPath.ArrayIndexNode(sub));
                    } else {
                        nodes.add(new RelaxedDataBinder.BeanPath.MapIndexNode(sub));
                    }
                }
                current = current.substring(endRef + 1);
                startRef = current.indexOf("[");
            }
            return current;
        }

        public void collapseKeys(int index) {
            List<PathNode> revised = new ArrayList<>();
            for (int i = 0; i < index; i++) {
                revised.add(this.nodes.get(i));
            }
            StringBuilder builder = new StringBuilder();
            for (int i = index; i < this.nodes.size(); i++) {
                if (i > index) {
                    builder.append(".");
                }
                builder.append(this.nodes.get(i).name);
            }
            revised.add(new RelaxedDataBinder.BeanPath.PropertyNode(builder.toString()));
            this.nodes = revised;
        }

        public void mapIndex(int index) {
            RelaxedDataBinder.BeanPath.PathNode node = this.nodes.get(index);
            if (node instanceof RelaxedDataBinder.BeanPath.PropertyNode) {
                node = ((RelaxedDataBinder.BeanPath.PropertyNode) node).mapIndex();
            }
            this.nodes.set(index, node);
        }

        public String prefix(int index) {
            return range(0, index);
        }

        public void rename(int index, String name) {
            this.nodes.get(index).name = name;
        }

        public String name(int index) {
            if (index < this.nodes.size()) {
                return this.nodes.get(index).name;
            }
            return null;
        }

        private String range(int start, int end) {
            StringBuilder builder = new StringBuilder();
            for (int i = start; i < end; i++) {
                RelaxedDataBinder.BeanPath.PathNode node = this.nodes.get(i);
                builder.append(node);
            }
            if (builder.toString().startsWith(("."))) {
                builder.replace(0, 1, "");
            }
            return builder.toString();
        }

        public boolean isArrayIndex(int index) {
            return this.nodes.get(index) instanceof RelaxedDataBinder.BeanPath.ArrayIndexNode;
        }

        public boolean isProperty(int index) {
            return this.nodes.get(index) instanceof RelaxedDataBinder.BeanPath.PropertyNode;
        }

        @Override
        public String toString() {
            return prefix(this.nodes.size());
        }

        private static class PathNode {

            protected String name;

            PathNode(String name){
                this.name = name;
            }

        }

        private static class ArrayIndexNode extends RelaxedDataBinder.BeanPath.PathNode {

            ArrayIndexNode(String name){
                super(name);
            }

            @Override
            public String toString() {
                return "[" + this.name + "]";
            }

        }

        private static class MapIndexNode extends RelaxedDataBinder.BeanPath.PathNode {

            MapIndexNode(String name){
                super(name);
            }

            @Override
            public String toString() {
                return "[" + this.name + "]";
            }

        }

        private static class PropertyNode extends RelaxedDataBinder.BeanPath.PathNode {

            PropertyNode(String name){
                super(name);
            }

            public RelaxedDataBinder.BeanPath.MapIndexNode mapIndex() {
                return new RelaxedDataBinder.BeanPath.MapIndexNode(this.name);
            }

            @Override
            public String toString() {
                return "." + this.name;
            }

        }

    }

    /**
     * Extended version of {@link BeanPropertyBindingResult} to support relaxed
     * binding.
     */
    private static class RelaxedBeanPropertyBindingResult extends BeanPropertyBindingResult {

        private RelaxedConversionService conversionService;

        RelaxedBeanPropertyBindingResult(Object target, String objectName, boolean autoGrowNestedPaths,
                                         int autoGrowCollectionLimit, ConversionService conversionService){
            super(target, objectName, autoGrowNestedPaths, autoGrowCollectionLimit);
            this.conversionService = new RelaxedConversionService(conversionService);
        }

        @Override
        protected BeanWrapper createBeanWrapper() {
            BeanWrapper beanWrapper = new RelaxedDataBinder.RelaxedBeanWrapper(getTarget());
            beanWrapper.setConversionService(this.conversionService);
            beanWrapper.registerCustomEditor(InetAddress.class, new InetAddressEditor());
            return beanWrapper;
        }

    }

    /**
     * Extended version of {@link BeanWrapperImpl} to support relaxed binding.
     */
    private static class RelaxedBeanWrapper extends BeanWrapperImpl {

        private static final Set<String> BENIGN_PROPERTY_SOURCE_NAMES;

        static {
            Set<String> names = new HashSet<>();
            names.add(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
            names.add(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
            BENIGN_PROPERTY_SOURCE_NAMES = Collections.unmodifiableSet(names);
        }

        RelaxedBeanWrapper(Object target){
            super(target);
        }

        @Override
        public void setPropertyValue(PropertyValue pv) throws BeansException {
            try {
                super.setPropertyValue(pv);
            } catch (NotWritablePropertyException ex) {
                PropertyOrigin origin = OriginCapablePropertyValue.getOrigin(pv);
                if (isBenign(origin)) {
                    logger.debug("Ignoring benign property binding failure", ex);
                    return;
                }
                if (origin == null) {
                    throw ex;
                }
                throw new RelaxedBindingNotWritablePropertyException(ex, origin);
            }
        }

        private boolean isBenign(PropertyOrigin origin) {
            String name = (origin != null ? origin.getSource().getName() : null);
            return BENIGN_PROPERTY_SOURCE_NAMES.contains(name);
        }

    }

    public static class RelaxedBindingNotWritablePropertyException extends NotWritablePropertyException {

        private final String         message;

        private final PropertyOrigin propertyOrigin;

        RelaxedBindingNotWritablePropertyException(NotWritablePropertyException ex, PropertyOrigin propertyOrigin){
            super(ex.getBeanClass(), ex.getPropertyName());
            this.propertyOrigin = propertyOrigin;
            this.message = "Failed to bind '" + propertyOrigin.getName() + "' from '"
                           + propertyOrigin.getSource().getName() + "' to '" + ex.getPropertyName() + "' property on '"
                           + ex.getBeanClass().getName() + "'";
        }

        @Override
        public String getMessage() {
            return this.message;
        }

        public PropertyOrigin getPropertyOrigin() {
            return this.propertyOrigin;
        }

    }
}
