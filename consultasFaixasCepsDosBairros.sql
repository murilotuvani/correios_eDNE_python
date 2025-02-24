use cep;
select * 
  from dne_localidade
 where cep like '18??????';
 
select a.loc_nu, a.loc_no, a.cep, a.loc_in_sit, a.loc_in_tipo_loc, b.val_nu, b.val_tx
  from dne_localidade a left join dne_localidade_variacao b on a.loc_nu=b.loc_nu
 where a.loc_no in ('BOITUVA','CABREUVA','CERQUILHO','INDAIATUABA','ITU','PORTO FELIZ','PIEDADE','SALTO','SOROCABA','TATUI')
   and a.ufe_sg='SP';
   
select * from dne_bairro;

select a.loc_nu, a.loc_no, a.loc_in_sit, a.loc_in_tipo_loc tipo
     , b.bai_nu, b.bai_no, b.bai_no_abrev
  from dne_localidade a left join dne_bairro b on a.loc_nu=b.loc_nu
 where a.loc_no in ('BOITUVA','CABREUVA','CERQUILHO','INDAIATUBA','ITU','PORTO FELIZ','PIEDADE','SALTO','SOROCABA','TATUI')
   and a.ufe_sg='SP'
 order by a.loc_no, b.bai_no, b.bai_nu;
 
select a.loc_nu, a.loc_no, a.loc_in_sit, a.loc_in_tipo_loc tipo
     , b.bai_nu, b.bai_no, b.bai_no_abrev
  from dne_localidade a left join dne_bairro b on a.loc_nu=b.loc_nu
 where a.loc_no in ('INDAIATUBA')
   and a.ufe_sg='SP'
 order by a.loc_no, b.bai_no, b.bai_nu;

select a.loc_nu, a.loc_no, a.loc_in_sit, a.loc_in_tipo_loc tipo
     , b.bai_nu, b.bai_no, b.bai_no_abrev
     , c.fcb_cep_ini
     , c.fcb_cep_fim
  from dne_localidade a left join dne_bairro b on a.loc_nu=b.loc_nu
                        left join dne_bairro_faixa c on c.bai_nu=b.bai_nu
 where a.loc_no in ('BOITUVA','CABREUVA','CERQUILHO','INDAIATUBA','ITU','PORTO FELIZ','PIEDADE','SALTO','SOROCABA','TATUI')
   and a.ufe_sg='SP'
 order by a.loc_no, b.bai_no, b.bai_nu, c.fcb_cep_ini;
 
 select * from cep.rotas_externas_bairros;
 
 select reb.Rotas
      , a.loc_no
      , b.bai_nu, b.bai_no, b.bai_no_abrev
      , c.fcb_cep_ini
      , c.fcb_cep_fim
   from cep.rotas_externas_bairros reb join cep.dne_bairro b on reb.bai_nu=b.bai_nu
                                       join cep.dne_bairro_faixa c on c.bai_nu=b.bai_nu
                                       join cep.dne_localidade a on b.loc_nu=a.loc_nu
  where reb.Rotas is not null
    AND c.fcb_cep_ini is not null;
  
select if(loc_nu=9260,12,0), Rotas, Rotas
  from cep.rotas_externas_bairros reb
 where reb.Rotas is not null
group by loc_nu, Rotas
order by loc_nu, Rotas;

insert into autogeral.expe_rota_exte (loja_id, nume, nome, modo, cria_em, cria_por, alte_em, alte_por) 
(select if(loc_nu=9260,12,0), Rotas, Rotas, 'IMEDIATA', now(), 'gabriel.ramos', now(), 'gabriel.ramos'
  from cep.rotas_externas_bairros reb
 where reb.Rotas is not null
group by loc_nu, Rotas
order by loc_nu, Rotas);


select re.EXPE_ROTA_EXTE_ID
      , concat_ws(' - Bairro N. ', b.bai_no, b.bai_nu) 
      , c.fcb_cep_ini
      , c.fcb_cep_fim
      , now(), 'gabriel.ramos', now(), 'gabriel.ramos'
   from cep.rotas_externas_bairros reb join cep.dne_bairro b on reb.bai_nu=b.bai_nu
                                       join cep.dne_bairro_faixa c on c.bai_nu=b.bai_nu
                                       join cep.dne_localidade a on b.loc_nu=a.loc_nu
                                       join autogeral.expe_rota_exte re on reb.Rotas=re.nume
  where reb.Rotas is not null
    AND c.fcb_cep_ini is not null
    and a.loc_nu=9260
    and re.LOJA_ID=12;
 
 insert into autogeral.expe_rota_exte_faix_cep (EXPE_ROTA_EXTE_ID, DSCR, CEP_INIC, CEP_FINA, cria_em, cria_por, alte_em, alte_por) 
 select re.EXPE_ROTA_EXTE_ID
      , concat_ws(' - Bairro N. ', b.bai_no, b.bai_nu) 
      , c.fcb_cep_ini
      , c.fcb_cep_fim
      , now(), 'gabriel.ramos', now(), 'gabriel.ramos'
   from cep.rotas_externas_bairros reb join cep.dne_bairro b on reb.bai_nu=b.bai_nu
                                       join cep.dne_bairro_faixa c on c.bai_nu=b.bai_nu
                                       join cep.dne_localidade a on b.loc_nu=a.loc_nu
                                       join autogeral.expe_rota_exte re on reb.Rotas=re.nume
  where reb.Rotas is not null
    AND c.fcb_cep_ini is not null
    and a.loc_nu=9260
    and re.LOJA_ID=12;