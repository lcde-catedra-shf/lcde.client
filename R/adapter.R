library(dplyr)
library(DBI)
library(RSQLite)

# constructors

adapter = function(
  path_to_db #: character
) {
  this = list()
  class(this) = 'adapter'

  this$path = path_to_db

  return(this)
}

# methods

.adapter.check_class = function(
  obj
) {
  if(!('adapter' %in% class(obj))) {
    stop("'obj' must be of type 'adapter'")
  }
}

.adapter.fetch_pca_data_schools = function(
  this, #: adapter
  nome_municipio, #: character
  sigla_uf, #: character
  rede, #: character
  etapa, #: character
  ano #: numeric
) {
  .adapter.check_class(this)

  conn = dbConnect(RSQLite::SQLite(), this$path)

  query = "
    SELECT
        ae.id,
        ae.nome_escola,
        ae.nome_municipio,
        ii.ano,
        ii.rede,
        ii.etapa,
        iap.lp,
        iap.mat,
        ii.np,
        ii.fluxo,
        it.tdi,
        c.latitude,
        c.longitude
    FROM
        amostras_escolas ae
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.np,
    			x.fluxo,
    			x.ano,
    			x.rede,
    			x.etapa
    		FROM indicadores_ideb x
    		inner join amostras_escolas as y on y.id = x.id_amostra
    		WHERE
    			x.ano == ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			y.nome_municipio = ? AND
    			y.uf = ?
        ) ii ON ii.id_amostra = ae.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.lp,
    			x.mat,
    			x.ano
    		FROM indicadores_aprendizado_adequado x
    		inner join amostras_escolas as y on y.id = x.id_amostra
    		WHERE
    			x.ano == ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			y.nome_municipio = ? AND
    			y.uf = ?
    	) iap ON
        	iap.id_amostra = ae.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.tdi,
    			x.ano
    		FROM indicadores_tdi x
    		inner join amostras_escolas as y on y.id = x.id_amostra
    		WHERE
    			x.ano == ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			y.nome_municipio = ? AND
    			y.uf = ?
        ) it ON
        	it.id_amostra = ae.id
    INNER JOIN
        (SELECT
    	    c.id_amostra,
    	    c.latitude,
    	    c.longitude
        FROM coordenadas c
        ) c on
        ae.id = c.id_amostra
  "

  data = dbGetQuery(
    conn,
    query,
    params = list(
      ano,
      etapa,
      rede,
      nome_municipio,
      sigla_uf,
      ano,
      etapa,
      rede,
      nome_municipio,
      sigla_uf,
      ano,
      etapa,
      rede,
      nome_municipio,
      sigla_uf
    )
  )
  dbDisconnect(conn)

  return(as.data.frame(data))
}

adapter.fetch_pca_data_schools = function(
    this, #: adapter
    nome_municipio, #: character
    sigla_uf, #: character vector
    redes, #: character vector
    etapas, #: character vector
    anos #: numeric vector
) {
  .adapter.check_class(this)

  data = NULL
  for(rede in redes) {
    for(etapa in etapas) {
      for(ano in anos) {

        data_i = this %>% .adapter.fetch_pca_data_schools(
          nome_municipio = nome_municipio,
          sigla_uf = sigla_uf,
          rede = rede,
          etapa = etapa,
          ano = ano
        )

        data = if(is.null(data)) data_i else rbind(data, data_i)
      }
    }
  }

  return(data)
}

adapter.fetch_municipality_boundary = function(
  this, #: adapter
  nome_municipio, #: character
  sigla_uf #: character
) {
  .adapter.check_class(this)

  conn = dbConnect(RSQLite::SQLite(), this$path)

  query = "
    SELECT m.malha FROM
      malhas m
    INNER JOIN amostras_municipios mun
    ON mun.id = m.id_amostra
    WHERE
      mun.nome_municipio = ? AND
      mun.uf = ?
  "

  data = dbGetQuery(
    conn,
    query,
    params = list(
      nome_municipio,
      sigla_uf
    )
  )

  dbDisconnect(conn)

  return(as.data.frame(data))
}

adapter.fetch_municipality_inse = function(
    this, #: adapter
    nome_municipio, #: character
    sigla_uf, #: character
    rede, #: character
    ano #: numeric
) {
  .adapter.check_class(this)

  conn = dbConnect(RSQLite::SQLite(), this$path)

  query = "
    SELECT
	    inse.id_amostra,
	    inse.inse,
	    c.longitude,
	    c.latitude
    FROM indicadores_inse inse
    INNER JOIN amostras_escolas esc on
	    esc.id = inse.id_amostra
    INNER JOIN coordenadas c on
	    c.id_amostra = esc.id
    WHERE
	    inse.ano = ? AND
	    inse.rede = ? AND
	    esc.nome_municipio = ? AND
	    esc.uf = ?
  "

  data = dbGetQuery(
    conn,
    query,
    params = list(
      ano,
      rede,
      nome_municipio,
      sigla_uf
    )
  )

  dbDisconnect(conn)

  return(as.data.frame(data))
}

adapter.fetch_pca_data_municipality = function(
  this, #: adapter
  nome_municipio,
  sigla_uf,
  rede,
  etapa,
  localizacao,
  ano
) {
  .adapter.check_class(this)

  conn = dbConnect(RSQLite::SQLite(), this$path)

  query = "
    SELECT
        am.id,
        am.nome_municipio,
        ii.ano,
        ii.rede,
        ii.etapa,
        iap.localizacao,
        iap.lp,
        iap.mat,
        ii.np,
        ii.fluxo,
        it.tdi
    FROM
        amostras_municipios am
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.np,
    			x.fluxo,
    			x.ano,
    			x.rede,
    			x.etapa
    		FROM indicadores_ideb x
    		inner join amostras_municipios y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			y.nome_municipio = ? AND
    			y.uf = ?
        ) ii ON ii.id_amostra = am.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.lp,
    			x.mat,
    			x.ano,
    			x.localizacao
    		FROM indicadores_aprendizado_adequado x
    		inner join amostras_municipios y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			x.localizacao = ? AND
    			y.nome_municipio = ? AND
    			y.uf = ?
    	) iap ON
        	iap.id_amostra = am.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.tdi,
    			x.ano
    		FROM indicadores_tdi x
    		inner join amostras_municipios y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			x.localizacao = ? AND
    			y.nome_municipio = ? AND
    			y.uf = ?
        ) it ON
        	it.id_amostra = am.id
  "

  data = dbGetQuery(
    conn,
    query,
    params = list(
      ano,
      etapa,
      rede,
      nome_municipio,
      sigla_uf,
      ano,
      etapa,
      rede,
      localizacao,
      nome_municipio,
      sigla_uf,
      ano,
      etapa,
      rede,
      localizacao,
      nome_municipio,
      sigla_uf
    )
  )

  dbDisconnect(conn)

  return(as.data.frame(data))
}

adapter.fetch_pca_data_state = function(
    this, #: adapter
    sigla_uf,
    rede,
    etapa,
    localizacao,
    ano
) {
  .adapter.check_class(this)

  conn = dbConnect(RSQLite::SQLite(), this$path)

  query = "
    SELECT
        ae.id,
        ae.uf,
        ae.nome_estado,
        ii.ano,
        ii.rede,
        ii.etapa,
        iap.localizacao,
        iap.lp,
        iap.mat,
        ii.np,
        ii.fluxo,
        it.tdi
    FROM
        amostras_estados ae
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.np,
    			x.fluxo,
    			x.ano,
    			x.rede,
    			x.etapa
    		FROM indicadores_ideb x
    		inner join amostras_estados y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			y.uf = ?
        ) ii ON ii.id_amostra = ae.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.lp,
    			x.mat,
    			x.ano,
    			x.localizacao
    		FROM indicadores_aprendizado_adequado x
    		inner join amostras_estados y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			x.localizacao = ? AND
    			y.uf = ?
    	) iap ON
        	iap.id_amostra = ae.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.tdi,
    			x.ano
    		FROM indicadores_tdi x
    		inner join amostras_estados y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			x.localizacao = ? AND
    			y.uf = ?
        ) it ON
        	it.id_amostra = ae.id
  "

  data = dbGetQuery(
    conn,
    query,
    params = list(
      ano,
      etapa,
      rede,
      sigla_uf,
      ano,
      etapa,
      rede,
      localizacao,
      sigla_uf,
      ano,
      etapa,
      rede,
      localizacao,
      sigla_uf
    )
  )

  dbDisconnect(conn)

  return(as.data.frame(data))
}

adapter.fetch_pca_data_country = function(
    this, #: adapter
    nome_pais,
    rede,
    etapa,
    localizacao,
    ano
) {
  .adapter.check_class(this)

  conn = dbConnect(RSQLite::SQLite(), this$path)

  query = "
    SELECT
        ap.id,
        ap.nome_pais,
        ii.ano,
        ii.rede,
        ii.etapa,
        iap.localizacao,
        iap.lp,
        iap.mat,
        ii.np,
        ii.fluxo,
        it.tdi
    FROM
        amostras_paises ap
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.np,
    			x.fluxo,
    			x.ano,
    			x.rede,
    			x.etapa
    		FROM indicadores_ideb x
    		inner join amostras_paises y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			y.nome_pais = ?
        ) ii ON ii.id_amostra = ap.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.lp,
    			x.mat,
    			x.ano,
    			x.localizacao
    		FROM indicadores_aprendizado_adequado x
    		inner join amostras_paises y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			x.localizacao = ? AND
    			y.nome_pais = ?
    	) iap ON
        	iap.id_amostra = ap.id
    INNER JOIN
        (
        	SELECT
    			x.id_amostra,
    			x.tdi,
    			x.ano
    		FROM indicadores_tdi x
    		inner join amostras_paises y on y.id = x.id_amostra
    		WHERE
    			x.ano = ? AND
    			x.etapa = ? AND
    			x.rede = ? AND
    			x.localizacao = ? AND
    			y.nome_pais = ?
        ) it ON
        	it.id_amostra = ap.id
  "

  data = dbGetQuery(
    conn,
    query,
    params = list(
      ano,
      etapa,
      rede,
      nome_pais,
      ano,
      etapa,
      rede,
      localizacao,
      nome_pais,
      ano,
      etapa,
      rede,
      localizacao,
      nome_pais
    )
  )

  dbDisconnect(conn)

  return(as.data.frame(data))
}
