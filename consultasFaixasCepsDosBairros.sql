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
   from cep.rotas_externas_bairros reb left join cep.dne_bairro b on reb.bai_nu=b.bai_nu
                                       left join dne_bairro_faixa c on c.bai_nu=b.bai_nu
                                       left join dne_localidade a on b.loc_nu=a.loc_nu
  where reb.Rotas is not null;
 