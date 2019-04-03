package com.alibaba.otter.canal.client.adapter.config.bind;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.util.Assert;

/**
 * Internal {@link ConversionService} used by {@link RelaxedDataBinder} to
 * support additional relaxed conversion.
 *
 * @author Phillip Webb
 * @author Stephane Nicoll
 * @since 1.1.0
 */
class RelaxedConversionService implements ConversionService {

    private final ConversionService        conversionService;

    private final GenericConversionService additionalConverters;

    /**
     * Create a new {@link RelaxedConversionService} instance.
     *
     * @param conversionService and option root conversion service
     */
    RelaxedConversionService(ConversionService conversionService){
        this.conversionService = conversionService;
        this.additionalConverters = new GenericConversionService();
        DefaultConversionService.addCollectionConverters(this.additionalConverters);
        this.additionalConverters
            .addConverterFactory(new RelaxedConversionService.StringToEnumIgnoringCaseConverterFactory());
        this.additionalConverters.addConverter(new StringToCharArrayConverter());
    }

    @Override
    public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
        return (this.conversionService != null && this.conversionService.canConvert(sourceType, targetType))
               || this.additionalConverters.canConvert(sourceType, targetType);
    }

    @Override
    public boolean canConvert(TypeDescriptor sourceType, TypeDescriptor targetType) {
        return (this.conversionService != null && this.conversionService.canConvert(sourceType, targetType))
               || this.additionalConverters.canConvert(sourceType, targetType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convert(Object source, Class<T> targetType) {
        Assert.notNull(targetType, "The targetType to convert to cannot be null");
        return (T) convert(source, TypeDescriptor.forObject(source), TypeDescriptor.valueOf(targetType));
    }

    @Override
    public Object convert(Object source, TypeDescriptor sourceType, TypeDescriptor targetType) {
        if (this.conversionService != null) {
            try {
                return this.conversionService.convert(source, sourceType, targetType);
            } catch (ConversionFailedException ex) {
                // Ignore and try the additional converters
            }
        }
        return this.additionalConverters.convert(source, sourceType, targetType);
    }

    /**
     * Clone of Spring's package private StringToEnumConverterFactory, but ignoring
     * the case of the source.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static class StringToEnumIgnoringCaseConverterFactory implements ConverterFactory<String, Enum> {

        @Override
        public <T extends Enum> Converter<String, T> getConverter(Class<T> targetType) {
            Class<?> enumType = targetType;
            while (enumType != null && !enumType.isEnum()) {
                enumType = enumType.getSuperclass();
            }
            Assert.notNull(enumType, "The target type " + targetType.getName() + " does not refer to an enum");
            return new RelaxedConversionService.StringToEnumIgnoringCaseConverterFactory.StringToEnum(enumType);
        }

        private static class StringToEnum<T extends Enum> implements Converter<String, T> {

            private final Class<T> enumType;

            StringToEnum(Class<T> enumType){
                this.enumType = enumType;
            }

            @Override
            public T convert(String source) {
                if (source.isEmpty()) {
                    // It's an empty enum identifier: reset the enum value to null.
                    return null;
                }
                source = source.trim();
                for (T candidate : (Set<T>) EnumSet.allOf(this.enumType)) {
                    RelaxedNames names = new RelaxedNames(
                        candidate.name().replace('_', '-').toLowerCase(Locale.ENGLISH));
                    for (String name : names) {
                        if (name.equals(source)) {
                            return candidate;
                        }
                    }
                    if (candidate.name().equalsIgnoreCase(source)) {
                        return candidate;
                    }
                }
                throw new IllegalArgumentException(
                    "No enum constant " + this.enumType.getCanonicalName() + "." + source);
            }

        }

    }

}
