package com.alibaba.otter.canal.spi;

/**
 * Use java service provider mechanism to provide {@link CanalMetricsService}.
 * <pre>
 * Example:
 * {@code
 *     ServiceLoader<CanalMetricsProvider> providers = ServiceLoader.load(CanalMetricsProvider.class);
 *     List<CanalMetricsProvider> list = new ArrayList<CanalMetricsProvider>();
 *     for (CanalMetricsProvider provider : providers) {
 *         list.add(provider);
 *     }
 * }
 * </pre>
 * @author Chuanyi Li
 */
public interface CanalMetricsProvider {

    /**
     * @return Impl of {@link CanalMetricsService}
     */
    CanalMetricsService getService();

}
