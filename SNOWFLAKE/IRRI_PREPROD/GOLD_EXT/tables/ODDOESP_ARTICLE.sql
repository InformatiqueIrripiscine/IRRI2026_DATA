create or replace dynamic table IRRI_PREPROD.GOLD_EXT.ODOOESP_ARTICLE(
	CODE_ARTICLE,
	DESIGNATION,
	ACHETABLE,
	VENDABLE,
	DISPO_CAISSE,
	TYPE_ARTICLE,
	TARIF_CESSION,
	TARIF_VENTE,
	CODE_BARRE,
	NOTE_INTERNE,
	CATEGORIE,
	CATEGORIE_1_CODE,
	CATEGORIE_1_LABEL,
	CATEGORIE_2_CODE,
	CATEGORIE_2_LABEL,
	CATEGORIE_3_CODE,
	CATEGORIE_3_LABEL,
	SUIVI_INVENTAIRE,
	FOUNISSEUR_PRINCIPAL,
	DIRECT_FOURNISSEUR,
	UNITE,
	SUBSTITUTION,
	DATE_UPDATE
) target_lag = '15 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = COMPUTE_WH
 as
--sélection des données Article pour Odoo    
Select distinct
    Articlex3.code as Code_Article,
    COALESCE(esLibelle.CLEANED_ATTRIBUTE_VALUE, ARTICLEX3.LIBELLE) as Designation,
    CASE WHEN
        tarifCession.CODE_ARTICLE is not null --il y a un tarif de cession
        AND Articlex3.ARTICLE_GERE_STOCK = true --géré en stock = oui
        AND Articlex3.EXCLUSION_MAGASIN = false -- Exclusion Magasin = non
        AND Articlex3.ARTICLE_COMPLET = true --Article Complet = oui
        AND Articlex3.CATEGORIE_X3 <> 'N19' --N'est pas du Sur-Mesure
        AND Articlex3.STATUT_MAGASIN_ID = 1 --Statut Magasin = Actif
        AND Articlex3.STATUT_CENTRALE_ID in (1,4) --Statut Centrale Actif ou Non renouvellé
        THEN true
        ELSE false END
        as Achetable, --TODO géré le cas où tarifg de cession est par conditionnement obligatoire
    CASE WHEN
        tarifVente.CODE_ARTICLE is not null --j'ai un tarif de vente
        AND ArticleX3.ARTICLE_COMPLET=true
        AND Articlex3.STATUT_MAGASIN_ID <> 3 --Statut Magasin n'est pas Non Vendable
        AND Articlex3.STATUT_CENTRALE_ID <> 2 --Statut Centrale n'est pas Elaboration
        THEN true
        ELSE false END 
        as Vendable,         
    CASE WHEN
        tarifVente.CODE_ARTICLE is not null --j'ai un tarif de vente
        AND ArticleX3.ARTICLE_COMPLET=true
        AND Articlex3.STATUT_MAGASIN_ID <> 3 --Statut Magasin n'est pas Non Vendable
        AND Articlex3.STATUT_CENTRALE_ID <> 2 --Statut Centrale n'est pas Elaboration
        AND Articlex3.CATEGORIE_X3 <> 'N19' -- l'article n'est pas Sur Mesure
        THEN true
        ELSE false END 
        as Dispo_caisse, --TODO
    CASE Articlex3.ARTICLE_GERE_STOCK when true then 'consu' else 'service' end as Type_Article,  --TODO
    tarifCession.TARIF_HT_CESS_ESPAGNE as Tarif_Cession,
    tarifVente.TARIF_TTC_VENTE_ESPAGNE as tarif_vente,
    Articlex3.FOURNISSEUR_EAN as Code_Barre,
    'https://irripim.irrijardin.com/product?id=' || Articlex3.code as Note_interne,
    esFamille.CLEANED_ATTRIBUTE_VALUE || '/' || esSousFamille.CLEANED_ATTRIBUTE_VALUE || '/' || esSousSousFamille.CLEANED_ATTRIBUTE_VALUE as Categorie,
    replace(esFamille.ATTRIBUTE_DATA, '"', '') as Categorie_1_Code,
    esFamille.CLEANED_ATTRIBUTE_VALUE  as Categorie_1_Label,
    replace(esSousFamille.ATTRIBUTE_DATA, '"', '')  as Categorie_2_Code,
    esSousFamille.CLEANED_ATTRIBUTE_VALUE  as Categorie_2_Label,
    replace(esSousSousFamille.ATTRIBUTE_DATA, '"', '') as Categorie_3_Code,
    esSousSousFamille.CLEANED_ATTRIBUTE_VALUE as Categorie_3_Label,
    --si l'article est géré en stock et n'est pas de type Frais généraux, alors on gère le suivi de stock, sinon, non
    CASE WHEN Articlex3.ARTICLE_GERE_STOCK AND Articlex3.FAMILLE_X3 <> 'FRG' THEN true else false end as Suivi_Inventaire,
    CASE WHEN Articlex3.DIR_FOURNISSEUR = TRUE and Articlex3.ARTICLE_GERE_STOCK = true
        THEN Articlex3.FOURNISSEUR_CODE
        ELSE null
    end as Founisseur_principal, --on ne renseigne le fournisseur principal que si on est en DIR Fournisseur
    CASE Articlex3.ARTICLE_GERE_STOCK when true then Articlex3.DIR_FOURNISSEUR else false end as Direct_fournisseur,
    Articlex3.UNITE_VENTE as Unite,
    CASE WHEN len(Articlex3.SUBSTITUTION_CODE_ARTICLE) >0 and articlex3.substitution_date_activation<=current_date()
        THEN 'Este artículo ha sido sustituido por la referencia '||Articlex3.SUBSTITUTION_CODE_ARTICLE || ' desde el '||TO_CHAR(articlex3.substitution_date_activation, 'DD/MM/YYYY')
        ELSE ''
        END AS Substitution,
    GREATEST(Articlex3.DATE_HEURE_MAJ, articleInfos.DATE_MAJ_PRIX_ACHAT, articleInfos.DATE_HEURE_MAJ ,articleInfos.DATE_MAJ, articleInfos.DATE_CREATION) as Date_Update
FROM
    SILVER.ARTICLE_X3_ATTRIBUTES Articlex3 
    --filtre des articles éligibles espagne
INNER JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES ArticleEs ON Articlex3.CODE = ArticleEs.SKU
    AND ArticleEs.ATTRIBUTE_CODE = 'CANAUX_VENTE_POSSIBLES'
    AND ArticleEs.cleaned_attribute_value like '%ESPAGNE%' 
    --récupération de la designation commerciale en espagnol
LEFT JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES esLibelle ON Articlex3.CODE = esLibelle.SKU
    AND esLibelle.ATTRIBUTE_CODE = 'DESIGNATION_COMMERCIALE'
    --récupération libelle Famille X3
LEFT JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES esFamille ON Articlex3.CODE = esFamille.SKU
    AND esFamille.ATTRIBUTE_CODE = 'FAMILLE_X3'
    --récupération libelle Sous Famille X3    
LEFT JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES esSousFamille ON Articlex3.CODE = esSousFamille.SKU
    AND esSousFamille.ATTRIBUTE_CODE = 'SOUS_FAMILLE_X3'
    --récupération libelle Sous Sous Famille X3        
LEFT JOIN SILVER.ARTICLE_ATTRIBUTES_VALUE_ES esSousSousFamille ON Articlex3.CODE = esSousSousFamille.SKU
    AND esSousSousFamille.ATTRIBUTE_CODE = 'SOUS_SOUS_FAMILLE_X3'
    --récupération Tarif Achat
LEFT JOIN SILVER.TARIF_CESSION_ESPAGNE tarifCession On Articlex3.CODE = tarifCession.CODE_ARTICLE 
    --récupération Tarif Vente
LEFT JOIN SILVER.TARIF_VENTE_ESPAGNE tarifVente On Articlex3.CODE = tarifVente.CODE_ARTICLE 
    --Informations Article
LEFT Join SILVER.ARTICLE_COMPLET_INFO articleInfos on ArticleX3.CODE = articleInfos.CODE_ARTICLE
    ;

SELECT * FROM IRRI_PREPROD.GOLD_EXT.ODOOESP_ARTICLE where code_article = '';