<xsl:stylesheet
        xmlns:xsl=
                "http://www.w3.org/1999/XSL/Transform"
        version="1.0"
>

    <xsl:output method="xml"/>
    <xsl:template match="/pdv_liste/pdv">
            <xsl:apply-templates select="*"/>
    </xsl:template>


</xsl:stylesheet>