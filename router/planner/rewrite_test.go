package planner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModifyQuery(t *testing.T) {
	assert := assert.New(t)

	type tcase struct {
		name     string
		query    string
		colname  string
		nextval  int64
		expected string
		wantErr  bool
	}

	for _, tt := range []tcase{
		{
			name:     "InsertWithColumns",
			query:    "INSERT INTO test_table (col1, col2) VALUES (1, 2);",
			colname:  "col3",
			nextval:  42,
			expected: "INSERT INTO test_table (col3, col1, col2) VALUES (42, 1, 2);",
			wantErr:  false,
		},
		{
			name:     "InsertWithoutColumns",
			query:    "INSERT INTO test_table VALUES (1, 2);",
			colname:  "col3",
			nextval:  42,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "InsertEmptyValues",
			query:    "INSERT INTO test_table (col1, col2) VALUES;",
			colname:  "col3",
			nextval:  42,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "InsertSingleColumn",
			query:    "INSERT INTO test_table (col1) VALUES (1);",
			colname:  "col2",
			nextval:  99,
			expected: "INSERT INTO test_table (col2, col1) VALUES (99, 1);",
			wantErr:  false,
		},
		{
			name:     "InsertSingleColumn",
			query:    "INSERT INTO test_table (col1) VALUES (1),(2);",
			colname:  "col2",
			nextval:  99,
			expected: "INSERT INTO test_table (col2, col1) VALUES (99, 1), (100, 2);",
			wantErr:  false,
		},
		{
			name:     "InsertSingleColumn",
			query:    "INSERT INTO test_table (col1,col3) VALUES (1,19),(2,20);",
			colname:  "col2",
			nextval:  99,
			expected: "INSERT INTO test_table (col2, col1,col3) VALUES (99, 1,19), (100, 2,20);",
			wantErr:  false,
		},
		{
			name:     "NotAnInsertStatement",
			query:    "SELECT 1",
			colname:  "col2",
			nextval:  99,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "Comment",
			query:    "--ping;",
			colname:  "col2",
			nextval:  99,
			expected: "",
			wantErr:  true,
		},
		{
			name:     "InsertWithReturningClause",
			query:    "INSERT INTO meta.campaigns_ocb_rate (campaign_id, rates, updated_at) VALUES ($1, $2, $3::timestamp) RETURNING meta.campaigns_ocb_rate.id;",
			colname:  "created_at",
			nextval:  1234567890,
			expected: "INSERT INTO meta.campaigns_ocb_rate (created_at, campaign_id, rates, updated_at) VALUES (1234567890, $1, $2, $3::timestamp) RETURNING meta.campaigns_ocb_rate.id;",
			wantErr:  false,
		},
		{
			name:     "InsertWithArrayAndReturningClause",
			query:    `INSERT INTO meta.strategies (campaign_id, strategy_template_id, order_num, weight, variables, name, is_deleted, target_groups, target_groups_exclude, dynamic_params, updated_at, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, ARRAY[$8]::INTEGER[], ARRAY[$9]::INTEGER[], $10, $11::timestamp, $12::timestamp) RETURNING meta.strategies.id;`,
			colname:  "created_by",
			nextval:  987654321,
			expected: `INSERT INTO meta.strategies (created_by, campaign_id, strategy_template_id, order_num, weight, variables, name, is_deleted, target_groups, target_groups_exclude, dynamic_params, updated_at, created_at) VALUES (987654321, $1, $2, $3, $4, $5, $6, $7, ARRAY[$8]::INTEGER[], ARRAY[$9]::INTEGER[], $10, $11::timestamp, $12::timestamp) RETURNING meta.strategies.id;`,
			wantErr:  false,
		},
		{
			name: "ComplexInsert",
			query: `INSERT INTO meta.campaigns
  (
    uuid,
    title,
    hyphenated_title,
    subtitle,
    is_active,
    is_personal,
    date_start,
    date_end,
    age_rating,
    groups,
    order_num,
    block_conditions_show,
    block_conditions_content,
    block_full_conditions_show,
    block_full_conditions_content,
    block_juridical_conditions_show,
    block_juridical_conditions_content,
    block_retailers_show,
    block_retailers_content,
    block_coupon_retailer_show,
    block_coupon_conditions_show,
    block_extra_conditions_show,
    block_badge_show,
    customer,
    slug,
    meta_type,
    budget_spent,
    is_promo,
    campaign_type,
    properties,
    is_bomb,
    display_content,
    external_id
  )
VALUES
  (
    '1916538f-b795-4eec-a277-913d2282218e'::uuid,
    'Продам гараж',
    'Продам гараж',
    'default',
    true,
    false,
    '2025-10-07T18:16:02.556572+00:00'::timestamptz,
    '2025-10-09T18:16:02.556587+00:00'::timestamptz,
    0,
    ARRAY [8,7]::INTEGER[],
    799999,
    true,
    '<ul>\n <li>Lorem ipsum dolor sit amet</li>\n <li>Consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua</li>\n <li>Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat</li>\n</ul>\n',,
    true,
    '<h2>Lorem ipsum dolor sit amet</h2>\n<ol>\n <li>Consectetur adipiscing elit.</li>\n <li>Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.</li>\n <li>Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.</li>\n <li>Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.</li>\n</ol>\n',
    true,
    '<h2>Lorem ipsum dolor sit amet</h2>\n<ul>\n <li>Consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.</li>\n <li>Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.</li>\n</ul>\n<h2>Duis aute irure dolor in reprehenderit</h2>\n<ul>\n <li>Velit esse cillum dolore eu fugiat nulla pariatur.</li>\n <li>Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum.</li>\n <li>Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium.</li>\n <li>Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit.</li>\n</ul>\n<h2>Neque porro quisquam est qui dolorem ipsum</h2>\n<ul>\n <li>Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur <a href="https://example.com" target="_blank" rel="nofollow noopener">Lorem ipsum dolor sit amet</a>.</li>\n <li>Vel illum qui dolorem eum fugiat quo voluptas nulla pariatur.</li>\n <li>Ut enim ad minima veniam quis nostrum exercitationem ullam corporis suscipit laboriosam nisi ut aliquid ex ea commodi consequatur.</li>\n <li>Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur <a href="https://example.com/details" target="_blank" rel="nofollow noopener">Ut enim ad minim veniam</a>.</li>\n</ul>\n',
    false,
    '{}'::INTEGER[],
    false,
    false,
    false,
    false,
    NULL,
    '1916538f-b795-4eec-a277-913d2282218e',
    'IMMEDIATE',
    0,
    false,
    'ONLINE',
    '{
      "images": {
        "sq_image_url": {
          "real_s3": "ocb/epn_12345/1.jpeg",
          "dyn": "http://leo.ru/dyn/cr/ocb/epn_12345/1.jpeg"
        }
      },
      "online": {
        "id": "12345",
        "source": "EPN",
        "conditions": [
          {
            "title": "title",
            "new": {
              "from": 5.0,
              "to": 20.0,
              "type": "PERCENT"
            },
            "old": {
              "from": 5.0,
              "to": 15.0,
              "type": "PERCENT"
            }
          },
          {
            "title": "title",
            "new": {
              "from": 10.0,
              "to": 20.0,
              "type": "PERCENT"
            },
            "old": {
              "from": 10.0,
              "to": 15.0,
              "type": "PERCENT"
            }
          }
        ],
        "transfer_link": {
          "query": {
            "sub1": "user_id",
            "sub2": "campaign_id"
          },
          "base_url": "https://ex.com"
        },
        "offer_help_text": null,
        "store_description": "## 1"
      },
      "search": {
        "key_words": null
      },
      "btl_banner_bonus_image": "cb/default.png",
      "btl_banner_bonus_style": "background: #FFCCAC;",
      "btl_banner_bonus_content_style": "color: #FFFFFF;",
      "btl_active": true,
      "btl_action": "redirectToEpn",
      "btl_gift_url": "https://ex.com/?sub2=1916538f-b795-4eec-a277-913d2282218e"
    }',
    false,
    '{
      "meta": {
        "fullRules": {
          "tpl": "section",
          "params": {
            "blocks": [
              {
                "tpl": "list",
                "params": {
                  "items": [
                    "item1",
                    "item2"
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "unordered"
                }
              },
              {
                "tpl": "list",
                "params": {
                  "items": [
                    "item1",
                    "item2",
                    "item3",
                    "item4"
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "unordered"
                }
              },
              {
                "tpl": "list",
                "params": {
                  "items": [
                    {
                      "tpl": "raw",
                      "params": {
                        "value": "value"
                      }
                    },
                    "param1",
                    "param2",
                    {
                      "tpl": "raw",
                      "params": {
                        "value": "value"
                      }
                    }
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "unordered"
                }
              }
            ]
          }
        },
        "howTo": {
          "tpl": "section",
          "params": {
            "blocks": [
              {
                "tpl": "list",
                "params": {
                  "items": [
                    "item1",
                    "item2",
                    "item3",
                    "item4"
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "ordered"
                }
              }
            ]
          }
        }
      },
      "blocks": [
        {
          "tpl": "cb:bannerBonus",
          "key": "btl",
          "params": {
            "title": {
              "tpl": "string",
              "params": {
                "value": "value"
              }
            },
            "content": [
              {
                "tpl": "raw",
                "params": {
                  "value": "<p>value</p>"
                }
              },
              {
                "tpl": "button",
                "params": {
                  "text": "text",
                  "href": "https://ex.com/?sub2=1916538f-b795-4eec-a277-913d2282218e",
                  "style": "color: #000000; background: #ffffff; width: 100%;",
                  "action": "redirectToEpn"
                }
              }
            ],
            "style": "background: radial-gradient(50% 70.08% at 50% 29.92%, rgba(255, 255, 255, 0.32) 0%, rgba(255, 255, 255, 0) 76.08%), #FFCCAC;",
            "contentStyle": "color: #FFFFFF;",
            "imageUrl": "http://leo.ru/dyn/re/cb/default.png"
          }
        },
        {
          "tpl": "list",
          "params": {
            "items": [
              "item1",
              "item2",
              "item3"
            ],
            "titleLevel": 2,
            "title": "title"
          },
          "key": "conditions"
        }
      ],
      "labels": [
        {
          "value": 12,
          "type": "internal"
        }
      ]
    }',
    'online.EPN.12345'
  )
RETURNING meta.campaigns.id, meta.campaigns.uuid, meta.campaigns.title, meta.campaigns.properties`,
			colname: "id",
			nextval: 42,
			expected: `INSERT INTO meta.campaigns
  (id,
    uuid,
    title,
    hyphenated_title,
    subtitle,
    is_active,
    is_personal,
    date_start,
    date_end,
    age_rating,
    groups,
    order_num,
    block_conditions_show,
    block_conditions_content,
    block_full_conditions_show,
    block_full_conditions_content,
    block_juridical_conditions_show,
    block_juridical_conditions_content,
    block_retailers_show,
    block_retailers_content,
    block_coupon_retailer_show,
    block_coupon_conditions_show,
    block_extra_conditions_show,
    block_badge_show,
    customer,
    slug,
    meta_type,
    budget_spent,
    is_promo,
    campaign_type,
    properties,
    is_bomb,
    display_content,
    external_id
  )
VALUES
  (42,
    '1916538f-b795-4eec-a277-913d2282218e'::uuid,
    'Продам гараж',
    'Продам гараж',
    'default',
    true,
    false,
    '2025-10-07T18:16:02.556572+00:00'::timestamptz,
    '2025-10-09T18:16:02.556587+00:00'::timestamptz,
    0,
    ARRAY [8,7]::INTEGER[],
    799999,
    true,
    '<ul>\n <li>Lorem ipsum dolor sit amet</li>\n <li>Consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua</li>\n <li>Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat</li>\n</ul>\n',,
    true,
    '<h2>Lorem ipsum dolor sit amet</h2>\n<ol>\n <li>Consectetur adipiscing elit.</li>\n <li>Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.</li>\n <li>Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.</li>\n <li>Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.</li>\n</ol>\n',
    true,
    '<h2>Lorem ipsum dolor sit amet</h2>\n<ul>\n <li>Consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.</li>\n <li>Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.</li>\n</ul>\n<h2>Duis aute irure dolor in reprehenderit</h2>\n<ul>\n <li>Velit esse cillum dolore eu fugiat nulla pariatur.</li>\n <li>Excepteur sint occaecat cupidatat non proident sunt in culpa qui officia deserunt mollit anim id est laborum.</li>\n <li>Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium.</li>\n <li>Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit.</li>\n</ul>\n<h2>Neque porro quisquam est qui dolorem ipsum</h2>\n<ul>\n <li>Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur <a href="https://example.com" target="_blank" rel="nofollow noopener">Lorem ipsum dolor sit amet</a>.</li>\n <li>Vel illum qui dolorem eum fugiat quo voluptas nulla pariatur.</li>\n <li>Ut enim ad minima veniam quis nostrum exercitationem ullam corporis suscipit laboriosam nisi ut aliquid ex ea commodi consequatur.</li>\n <li>Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur <a href="https://example.com/details" target="_blank" rel="nofollow noopener">Ut enim ad minim veniam</a>.</li>\n</ul>\n',
    false,
    '{}'::INTEGER[],
    false,
    false,
    false,
    false,
    NULL,
    '1916538f-b795-4eec-a277-913d2282218e',
    'IMMEDIATE',
    0,
    false,
    'ONLINE',
    '{
      "images": {
        "sq_image_url": {
          "real_s3": "ocb/epn_12345/1.jpeg",
          "dyn": "http://leo.ru/dyn/cr/ocb/epn_12345/1.jpeg"
        }
      },
      "online": {
        "id": "12345",
        "source": "EPN",
        "conditions": [
          {
            "title": "title",
            "new": {
              "from": 5.0,
              "to": 20.0,
              "type": "PERCENT"
            },
            "old": {
              "from": 5.0,
              "to": 15.0,
              "type": "PERCENT"
            }
          },
          {
            "title": "title",
            "new": {
              "from": 10.0,
              "to": 20.0,
              "type": "PERCENT"
            },
            "old": {
              "from": 10.0,
              "to": 15.0,
              "type": "PERCENT"
            }
          }
        ],
        "transfer_link": {
          "query": {
            "sub1": "user_id",
            "sub2": "campaign_id"
          },
          "base_url": "https://ex.com"
        },
        "offer_help_text": null,
        "store_description": "## 1"
      },
      "search": {
        "key_words": null
      },
      "btl_banner_bonus_image": "cb/default.png",
      "btl_banner_bonus_style": "background: #FFCCAC;",
      "btl_banner_bonus_content_style": "color: #FFFFFF;",
      "btl_active": true,
      "btl_action": "redirectToEpn",
      "btl_gift_url": "https://ex.com/?sub2=1916538f-b795-4eec-a277-913d2282218e"
    }',
    false,
    '{
      "meta": {
        "fullRules": {
          "tpl": "section",
          "params": {
            "blocks": [
              {
                "tpl": "list",
                "params": {
                  "items": [
                    "item1",
                    "item2"
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "unordered"
                }
              },
              {
                "tpl": "list",
                "params": {
                  "items": [
                    "item1",
                    "item2",
                    "item3",
                    "item4"
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "unordered"
                }
              },
              {
                "tpl": "list",
                "params": {
                  "items": [
                    {
                      "tpl": "raw",
                      "params": {
                        "value": "value"
                      }
                    },
                    "param1",
                    "param2",
                    {
                      "tpl": "raw",
                      "params": {
                        "value": "value"
                      }
                    }
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "unordered"
                }
              }
            ]
          }
        },
        "howTo": {
          "tpl": "section",
          "params": {
            "blocks": [
              {
                "tpl": "list",
                "params": {
                  "items": [
                    "item1",
                    "item2",
                    "item3",
                    "item4"
                  ],
                  "title": "title",
                  "titleLevel": 2,
                  "type": "ordered"
                }
              }
            ]
          }
        }
      },
      "blocks": [
        {
          "tpl": "cb:bannerBonus",
          "key": "btl",
          "params": {
            "title": {
              "tpl": "string",
              "params": {
                "value": "value"
              }
            },
            "content": [
              {
                "tpl": "raw",
                "params": {
                  "value": "<p>value</p>"
                }
              },
              {
                "tpl": "button",
                "params": {
                  "text": "text",
                  "href": "https://ex.com/?sub2=1916538f-b795-4eec-a277-913d2282218e",
                  "style": "color: #000000; background: #ffffff; width: 100%;",
                  "action": "redirectToEpn"
                }
              }
            ],
            "style": "background: radial-gradient(50% 70.08% at 50% 29.92%, rgba(255, 255, 255, 0.32) 0%, rgba(255, 255, 255, 0) 76.08%), #FFCCAC;",
            "contentStyle": "color: #FFFFFF;",
            "imageUrl": "http://leo.ru/dyn/re/cb/default.png"
          }
        },
        {
          "tpl": "list",
          "params": {
            "items": [
              "item1",
              "item2",
              "item3"
            ],
            "titleLevel": 2,
            "title": "title"
          },
          "key": "conditions"
        }
      ],
      "labels": [
        {
          "value": 12,
          "type": "internal"
        }
      ]
    }',
    'online.EPN.12345'
  )
RETURNING meta.campaigns.id, meta.campaigns.uuid, meta.campaigns.title, meta.campaigns.properties`,
		},
	} {
		startV := tt.nextval
		t.Run(tt.name, func(t *testing.T) {
			result, err := RewriteReferenceRelationAutoIncInsert(tt.query, tt.colname, func() (string, error) {
				ret := startV
				startV++

				return fmt.Sprintf("%d", ret), nil
			})

			if tt.wantErr && err == nil {
				t.Errorf("ModifyQuery/%s expected error, got nil. Query: %s", tt.name, result)
				return
			}

			if err != nil {
				if (err != nil) != tt.wantErr {
					t.Errorf("ModifyQuery/%s error = %v, wantErr %v", tt.name, err, tt.wantErr)
				}
				return
			}

			assert.Equal(tt.expected, result)
		})
	}
}

func BenchmarkModifyQuery(b *testing.B) {
	query := "INSERT INTO table_name (col1, col3) VALUES (1, 3);"
	colname := "col2"
	nextval := "42"

	for b.Loop() {
		_, _ = RewriteReferenceRelationAutoIncInsert(query, colname, func() (string, error) {
			return nextval, nil
		})
	}
}
