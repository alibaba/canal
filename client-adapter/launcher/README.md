# 记录测试数据，插入订单表
```sql
INSERT INTO `order`.`order1` (`order_id`, `event_id`, `placed_by_id`, `placed_by`, `placed_method`, `order_type`, `is_renewed`, `has_after_sale`, `is_exchanged`, `has_gifts`, `is_group`, `delay_shipping`, `group_code`, `state`, `order_remark`, `customer_id`, `customer_phone_number`, `encrypted_customer_phone_number`, `hashed_customer_phone_number`, `customer_union_id`, `customer_open_id`, `third_party_order_id`, `platform`, `currency`, `overseas_order`, `paid_deposit`, `total_discount`, `original_total`, `subtotal`, `total`, `actual_payment`, `payment_option`, `first_paid_at`, `paid_at`, `expired_at`, `closed_at`, `cancelled_at`, `finished_at`, `cancellation_reason`, `closed_reason_id`, `closed_reason`, `review_state`, `payment_state`, `check_state`, `shipping_state`, `contract_id`, `business_type_id`, `business_unit_id`, `business_code`, `use_points`, `shipped_at`, `placed_at`, `updated_at`)
VALUES ('CM200430157802412232387889150', 2, 241873, '高兰test', 2, 2, 1, 0, 0, 0, 0, 0, '', 127, '自动化测试创建订单', 1000845917, '', '00$$sgCKSRQ0jc8Jfc6S3rt+OQ==', '4683d12884f5175b494409b538d1519e38341ac3e24e6c35ed406d078e8f85bb', '', '240701', '', 5, 'CNY', 0, 0.00, 0.00, 0.10, 0.10, 0.10, 0.10, 1, 1588215781, 1588215781, 0, 0, 0, 0, '', 0, '', 105, 206, 303, 403, 0, 1, 13, 'ROCKET_headteacher', 100000000, 1588215782, 1588215780, 1617250363);

INSERT INTO `order`.`order_performance` (`order_id`, `order_type`, `owner_id`, `owner`, `handled_by_id`, `handled_by`, `created_at`, `updated_at`)
VALUES ('CM200430157802412232387889150', 2, 241873, '高珂', 0, '', 1632981600, 1632981600);
```

