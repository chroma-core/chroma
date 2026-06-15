-- Add http_currents function for refreshing derived currents views via Foundation
INSERT INTO "public"."functions" ("id", "name", "is_incremental", "return_type", "is_async") VALUES (
  '24d9efcb-7c39-406d-8ea1-70ce1362c158',
  'http_currents',
  true,
  '{}',
  false
);
