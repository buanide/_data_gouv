import re

# Fonction pour extraire les dates au format dd/mm/yyyy

"""
ok pour : spark_compute_and_insert_adjustement_activity.hql
"""
def find_alias_after_parentheses(hive_query):
    """
    Trouve les alias qui suivent un FROM ou JOIN avec une sous-requête en parenthèses,
    en excluant 'AND' comme alias.

    Args:
        hive_query (str): La requête Hive à analyser.

    Returns:
        dict: Un dictionnaire où les clés sont les alias et les valeurs sont les sous-requêtes associées.
    """
    # Supprimer les sauts de ligne pour simplifier l'analyse
    hive_query = " ".join(hive_query.split())

    # Expression régulière ajustée pour capturer les alias sans capturer 'AND'
    # Utilisation d'un lookahead négatif pour éviter la capture de 'AND' comme alias
    pattern = r'(?:FROM|JOIN|LEFT\sJOIN)\s*\(([^()]*+(?:\([^()]*+\))*[^()]*+)\)\s+([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*AND\b)'

    # Trouver toutes les correspondances
    matches = re.findall(pattern, hive_query)

    # Afficher les correspondances pour vérifier
    print("matches:", matches)

    # Construire le dictionnaire alias -> sous-requête
    alias_dict = {match[1]: match[0] for match in matches}
    return alias_dict


def find_alias_after_parentheses(hive_query):
    """
    Trouve les alias qui suivent un FROM ou JOIN avec une sous-requête en parenthèses,
    en excluant 'AND' comme alias.

    Args:
        hive_query (str): La requête Hive à analyser.

    Returns:
        dict: Un dictionnaire {alias: sous-requête}.
    """
    # Supprimer les sauts de ligne pour simplifier l'analyse
    hive_query = " ".join(hive_query.split())

    # Expression régulière pour capturer les alias sur une sous-requête parenthésée
    # On évite de capter 'AND' comme alias (lookahead négatif)
    pattern = (
        r"(?:FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN)\s*"
        r"\(([^()]*+(?:\([^()]*+\))*[^()]*+)\)\s+([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*AND\b)"
    )

    matches = re.findall(pattern, hive_query, flags=re.IGNORECASE)
    # Exemple: [("SELECT * FROM CDR.SPARK_IT_ZTE_ADJUSTMENT ...", "A"), ( ... )]

    # Construire le dictionnaire alias -> sous-requête
    alias_dict = {match[1].upper(): match[0] for match in matches}
    return alias_dict

hql="""
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
    COMMERCIAL_OFFER_CODE
     , TRANSACTION_TYPE
     , SUB_ACCOUNT
     , TRANSACTION_SIGN
     , SOURCE_PLATFORM
     , SOURCE_DATA
     , SERVED_SERVICE
     , SERVICE_CODE
     , DESTINATION_CODE
     , SERVED_LOCATION
     , MEASUREMENT_UNIT
     , RATED_COUNT
     , RATED_VOLUME
     , TAXED_AMOUNT
     , UNTAXED_AMOUNT
     , INSERT_DATE
     , TRAFFIC_MEAN
     , OPERATOR_CODE
     , NULL LOCATION_CI
     , TRANSACTION_DATE
FROM(
        SELECT
            DEACTIVATION_DATE TRANSACTION_DATE
             ,UPPER(PROFILE) COMMERCIAL_OFFER_CODE
             ,'DEACTIVATED_ACCOUNT_BALANCE' TRANSACTION_TYPE
             ,'MAIN' SUB_ACCOUNT
             ,'-' TRANSACTION_SIGN
             , 'IN' SOURCE_PLATFORM
             ,'FT_CONTRACT_SNAPSHOT'  SOURCE_DATA
             , 'IN_ACCOUNT' SERVED_SERVICE
             , 'NVX_BALANCE' SERVICE_CODE
             , 'DEST_ND' DESTINATION_CODE
             , NULL SERVED_LOCATION
             ,'HIT' MEASUREMENT_UNIT
             , SUM (1) RATED_COUNT
             , SUM (1) RATED_VOLUME
             , SUM (MAIN_CREDIT) TAXED_AMOUNT
             , SUM ((1-0.1925) * MAIN_CREDIT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             ,'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE OPERATOR_CODE
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND MAIN_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
        UNION
        SELECT
            DEACTIVATION_DATE TRANSACTION_DATE
             ,UPPER(PROFILE) COMMERCIAL_OFFER_CODE
             ,'DEACTIVATED_ACCOUNT_BALANCE' TRANSACTION_TYPE
             ,'PROMO' SUB_ACCOUNT
             ,'-' TRANSACTION_SIGN
             , 'IN' SOURCE_PLATFORM
             ,'FT_CONTRACT_SNAPSHOT'  SOURCE_DATA
             , 'IN_ACCOUNT' SERVED_SERVICE
             , 'NVX_BALANCE' SERVICE_CODE
             , 'DEST_ND' DESTINATION_CODE
             , NULL SERVED_LOCATION
             ,'HIT' MEASUREMENT_UNIT
             , SUM (1) RATED_COUNT
             , SUM (1) RATED_VOLUME
             , SUM (PROMO_CREDIT) TAXED_AMOUNT
             , SUM ((1-0.1925) * PROMO_CREDIT) UNTAXED_AMOUNT
             , CURRENT_TIMESTAMP INSERT_DATE
             ,'REVENUE' TRAFFIC_MEAN
             , OPERATOR_CODE OPERATOR_CODE
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND PROMO_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
    ) A
"""

# Affichage des dates trouvées
cor=find_alias_after_parentheses(hql)


print(cor)