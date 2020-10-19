package com.alibaba.otter.canal.client.adapter.config.common;

import java.util.*;

import org.springframework.core.env.Environment;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * {@link YamlProcessor.DocumentMatcher} backed by
 * {@link Environment#getActiveProfiles()}. A YAML document may define a
 * "spring.profiles" element as a comma-separated list of Spring profile names,
 * optionally negated using the {@code !} character. If both negated and
 * non-negated profiles are specified for a single document, at least one
 * non-negated profile must match and no negated profiles may match.
 *
 * @author Dave Syer
 * @author Matt Benson
 * @author Phillip Webb
 * @author Andy Wilkinson
 */
public class SpringProfileDocumentMatcher implements YamlProcessor.DocumentMatcher {

    private String[] activeProfiles = new String[0];

    public SpringProfileDocumentMatcher(){
    }

    public SpringProfileDocumentMatcher(String... profiles){
        addActiveProfiles(profiles);
    }

    public void addActiveProfiles(String... profiles) {
        LinkedHashSet<String> set = new LinkedHashSet<>(Arrays.asList(this.activeProfiles));
        Collections.addAll(set, profiles);
        this.activeProfiles = set.toArray(new String[set.size()]);
    }

    @Override
    public YamlProcessor.MatchStatus matches(Properties properties) {
        List<String> profiles = extractSpringProfiles(properties);
        ProfilesMatcher profilesMatcher = getProfilesMatcher();
        Set<String> negative = extractProfiles(profiles, ProfileType.NEGATIVE);
        Set<String> positive = extractProfiles(profiles, ProfileType.POSITIVE);
        if (!CollectionUtils.isEmpty(negative)) {
            if (profilesMatcher.matches(negative) == YamlProcessor.MatchStatus.FOUND) {
                return YamlProcessor.MatchStatus.NOT_FOUND;
            }
            if (CollectionUtils.isEmpty(positive)) {
                return YamlProcessor.MatchStatus.FOUND;
            }
        }
        return profilesMatcher.matches(positive);
    }

    private List<String> extractSpringProfiles(Properties properties) {
        SpringProperties springProperties = new SpringProperties();
        MutablePropertySources propertySources = new MutablePropertySources();
        propertySources.addFirst(new PropertiesPropertySource("profiles", properties));
        // PropertyValues propertyValues = new PropertySourcesPropertyValues(
        // propertySources);
        // new RelaxedDataBinder(springProperties, "spring").bind(propertyValues);
        // TODO
        List<String> profiles = springProperties.getProfiles();
        return profiles;
    }

    private ProfilesMatcher getProfilesMatcher() {
        return (this.activeProfiles.length != 0 ? new ActiveProfilesMatcher(
                new HashSet<>(Arrays.asList(this.activeProfiles))) : new EmptyProfilesMatcher());
    }

    private Set<String> extractProfiles(List<String> profiles, ProfileType type) {
        if (CollectionUtils.isEmpty(profiles)) {
            return null;
        }
        Set<String> extractedProfiles = new HashSet<>();
        for (String candidate : profiles) {
            ProfileType candidateType = ProfileType.POSITIVE;
            if (candidate.startsWith("!")) {
                candidateType = ProfileType.NEGATIVE;
            }
            if (candidateType == type) {
                extractedProfiles.add(type != ProfileType.POSITIVE ? candidate.substring(1) : candidate);
            }
        }
        return extractedProfiles;
    }

    /**
     * Profile match types.
     */
    enum ProfileType {

                      POSITIVE, NEGATIVE

    }

    /**
     * Base class for profile matchers.
     */
    private abstract static class ProfilesMatcher {

        public final YamlProcessor.MatchStatus matches(Set<String> profiles) {
            if (CollectionUtils.isEmpty(profiles)) {
                return YamlProcessor.MatchStatus.ABSTAIN;
            }
            return doMatches(profiles);
        }

        protected abstract YamlProcessor.MatchStatus doMatches(Set<String> profiles);

    }

    /**
     * {@link ProfilesMatcher} that matches when a value in {@code spring.profiles}
     * is also in {@code spring.profiles.active}.
     */
    private static class ActiveProfilesMatcher extends ProfilesMatcher {

        private final Set<String> activeProfiles;

        ActiveProfilesMatcher(Set<String> activeProfiles){
            this.activeProfiles = activeProfiles;
        }

        @Override
        protected YamlProcessor.MatchStatus doMatches(Set<String> profiles) {
            if (profiles.isEmpty()) {
                return YamlProcessor.MatchStatus.NOT_FOUND;
            }
            for (String activeProfile : this.activeProfiles) {
                if (profiles.contains(activeProfile)) {
                    return YamlProcessor.MatchStatus.FOUND;
                }
            }
            return YamlProcessor.MatchStatus.NOT_FOUND;
        }

    }

    /**
     * {@link ProfilesMatcher} that matches when {@code
     * spring.profiles} is empty or contains a value with no text.
     *
     * @see StringUtils#hasText(String)
     */
    private static class EmptyProfilesMatcher extends ProfilesMatcher {

        @Override
        public YamlProcessor.MatchStatus doMatches(Set<String> springProfiles) {
            if (springProfiles.isEmpty()) {
                return YamlProcessor.MatchStatus.FOUND;
            }
            for (String profile : springProfiles) {
                if (!StringUtils.hasText(profile)) {
                    return YamlProcessor.MatchStatus.FOUND;
                }
            }
            return YamlProcessor.MatchStatus.NOT_FOUND;
        }

    }

    /**
     * Class for binding {@code spring.profiles} property.
     */
    static class SpringProperties {

        private List<String> profiles = new ArrayList<>();

        public List<String> getProfiles() {
            return this.profiles;
        }

        public void setProfiles(List<String> profiles) {
            this.profiles = profiles;
        }

    }

}
