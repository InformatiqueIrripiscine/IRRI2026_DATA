create or replace dynamic table IRRI_PROD.SILVER.ARTICLE_ATTRIBUTES_VALUE_FR(
	SKU,
	ATTRIBUTE_CODE,
	ATTRIBUTE_LABEL_FR_FR,
	ATTRIBUTE_DATA,
	ATTRIBUTE_LOCALE,
	ATTRIBUTE_SCOPE,
	CLEANED_ATTRIBUTE_VALUE,
	COMPLETED_ATTRIBUTE_VALUE,
	LABEL_FR_FR
) target_lag = '1 minute' refresh_mode = INCREMENTAL initialize = ON_CREATE warehouse = COMPUTE_WH
 as
WITH
products_base AS (
    SELECT
        p.SKU,
        p.ITEM:family::STRING AS family_code,
        p.ITEM:parent::STRING AS parent_product_code,
        p.ITEM:categories AS categories_array,
        p.ITEM:groups AS groups_array,
        p.ITEM:associations AS associations_object,
        p.ITEM:values AS attributes_values,
        p.ITEM:enabled::BOOLEAN AS is_enabled,
        p.ITEM:created::TIMESTAMP_NTZ AS created_at,
        p.ITEM:updated::TIMESTAMP_NTZ AS updated_at,
        p.ITEM:metadata:workflow_status::STRING AS workflow_status,
        p.ITEM:"_links":"self":"href"::STRING AS api_url
    FROM BRONZE.AKENEO_PRODUCTS_RAW_DATA AS p
    ORDER BY 1
),

flattened_attributes AS (
    SELECT
        p.SKU,
        attr.key::STRING AS attribute_code,
        val.value:data AS attribute_data,
        val.value:locale::STRING AS attribute_locale,
        val.value:scope::STRING AS attribute_scope,
        CASE
            WHEN IS_OBJECT(attribute_data) AND attribute_data:amount IS NOT NULL AND attribute_data:unit IS NULL
                THEN attribute_data:amount
            WHEN IS_OBJECT(attribute_data) AND attribute_data:amount IS NOT NULL AND attribute_data:unit IS NOT NULL
                THEN CONCAT(TO_VARCHAR(attribute_data:amount),' ',TO_VARCHAR(attribute_data:unit))
            WHEN IS_ARRAY(attribute_data)
                THEN ARRAY_TO_STRING(attribute_data, ', ')
            ELSE
                CASE WHEN attribute_code IN ('COMMENTAIRE_DEVIS_IRRIDEVIS','DESCRIPTIF_COMMERCIAL_LONG','DESCRIPTIF_COMMERCIAL_LONG_1','DESCRIPTIF_COMMERCIAL_LONG_2','DESCRIPTIF_COMMERCIAL_LONG_3','DESCRIPTIF_COMMERCIAL_LONG_4','DESCRIPTIF_COMMERCIAL_LONG_5','DESIGNATION_COMMERCIALE','DESIGNATION_WEB_SEO','DESIGNATION_WEB_SEO_PARENT','DESIGNATION_X3','FOURNISSEUR_DESCRIPTIF_COMMERCIAL','LES_CONSEILS_DE_PRO_TEXTE','LES_PLUS_PRODUIT_1','LES_PLUS_PRODUIT_2','LES_PLUS_PRODUIT_3','LES_PLUS_PRODUIT_4','LES_PLUS_PRODUIT_5','LES_PLUS_PRODUIT_6','LIBELLE_ACCESOIRE_INCLUS_1','LIBELLE_ACCESOIRE_INCLUS_10','LIBELLE_ACCESOIRE_INCLUS_2','LIBELLE_ACCESOIRE_INCLUS_3','LIBELLE_ACCESOIRE_INCLUS_4','LIBELLE_ACCESOIRE_INCLUS_5','LIBELLE_ACCESOIRE_INCLUS_6','LIBELLE_ACCESOIRE_INCLUS_7','LIBELLE_ACCESOIRE_INCLUS_8','LIBELLE_ACCESOIRE_INCLUS_9','STATUT_CENTRALE','STATUT_COMARCH','STATUT_MAGASIN')
                    THEN CLEAN_HTML_TAGS_JS(decode_text(attribute_data))
                ELSE decode_text(attribute_data)
                END
        END AS cleaned_attribute_value
    FROM
        products_base p,
        LATERAL FLATTEN(input => p.attributes_values) AS attr,
        LATERAL FLATTEN(input => attr.value) AS val

    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY p.sku, attribute_code
        ORDER BY
            (CASE WHEN attribute_locale = 'fr_FR' THEN 1 WHEN attribute_locale IS NULL THEN 2 ELSE 3 END),
            (CASE WHEN attribute_scope = 'digital' THEN 1 WHEN attribute_scope IS NULL THEN 2 ELSE 3 END)
    ) = 1
)

SELECT
    f.SKU,
    f.attribute_code,
    a.RAW_DATA:labels:fr_FR::STRING AS attribute_label_fr_fr,
    f.attribute_data,
    f.attribute_locale,
    f.attribute_scope,
    COALESCE(o.RAW_DATA:labels:fr_FR::STRING, f.cleaned_attribute_value) AS cleaned_attribute_value,
    CONCAT(a.RAW_DATA:labels:fr_FR::STRING, ' : ', COALESCE(o.RAW_DATA:labels:fr_FR::STRING, f.cleaned_attribute_value)) AS completed_attribute_value,
    o.RAW_DATA:labels:fr_FR::STRING AS label_fr_fr
FROM flattened_attributes f
LEFT JOIN BRONZE.AKENEO_ATTRIBUTES_RAW_DATA a
    ON a.RAW_DATA:code::STRING = f.attribute_code
LEFT JOIN BRONZE.AKENEO_OPTIONS_ATTRIBUTES_RAW_DATA o
    ON o.RAW_DATA:attribute::STRING = f.attribute_code
    AND o.RAW_DATA:code::STRING = f.cleaned_attribute_value
;